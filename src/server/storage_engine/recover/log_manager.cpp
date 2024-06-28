#include "include/storage_engine/recover/log_manager.h"

#include <unordered_set>

#include "include/storage_engine/transaction/trx.h"

RC LogEntryIterator::init(LogFile &log_file)
{
  log_file_ = &log_file;
  return RC::SUCCESS;
}

RC LogEntryIterator::next()
{
  LogEntryHeader header;
  RC rc = log_file_->read(reinterpret_cast<char *>(&header), sizeof(header));
  if (rc != RC::SUCCESS) {
    if (log_file_->eof()) {
      return RC::RECORD_EOF;
    }
    LOG_WARN("failed to read log header. rc=%s", strrc(rc));
    return rc;
  }

  char *data = nullptr;
  int32_t entry_len = header.log_entry_len_;
  if (entry_len > 0) {
    data = new char[entry_len];
    rc = log_file_->read(data, entry_len);
    if (RC_FAIL(rc)) {
      LOG_WARN("failed to read log data. data size=%d, rc=%s", entry_len, strrc(rc));
      delete[] data;
      data = nullptr;
      return rc;
    }
  }

  if (log_entry_ != nullptr) {
    delete log_entry_;
    log_entry_ = nullptr;
  }
  log_entry_ = LogEntry::build(header, data);
  delete[] data;
  return rc;
}

bool LogEntryIterator::valid() const
{
  return log_entry_ != nullptr;
}

const LogEntry &LogEntryIterator::log_entry()
{
  return *log_entry_;
}

////////////////////////////////////////////////////////////////////////////////

LogManager::~LogManager()
{
  if (log_buffer_ != nullptr) {
    delete log_buffer_;
    log_buffer_ = nullptr;
  }

  if (log_file_ != nullptr) {
    delete log_file_;
    log_file_ = nullptr;
  }
}

RC LogManager::init(const char *path)
{
  log_buffer_ = new LogBuffer();
  log_file_   = new LogFile();
  return log_file_->init(path);
}

RC LogManager::append_begin_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_BEGIN, trx_id));
}

RC LogManager::append_rollback_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_ROLLBACK, trx_id));
}

RC LogManager::append_commit_trx_log(int32_t trx_id, int32_t commit_xid)
{
  RC rc = append_log(LogEntry::build_commit_entry(trx_id, commit_xid));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to append trx commit log. trx id=%d, rc=%s", trx_id, strrc(rc));
    return rc;
  }
  rc = sync(); // 事务提交时需要把当前事务关联的日志项都写入到磁盘中，这样做是保证不丢数据
  return rc;
}

RC LogManager::append_record_log(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data)
{
  LogEntry *log_entry = LogEntry::build_record_entry(type, trx_id, table_id, rid, data_len, data_offset, data);
  if (nullptr == log_entry) {
    LOG_WARN("failed to create log entry");
    return RC::NOMEM;
  }
  return append_log(log_entry);
}

RC LogManager::append_log(LogEntry *log_entry)
{
  if (nullptr == log_entry) {
    return RC::INVALID_ARGUMENT;
  }
  return log_buffer_->append_log_entry(log_entry);
}

RC LogManager::sync()
{
  return log_buffer_->flush_buffer(*log_file_);
}

RC LogManager::recover(Db *db)
{
  TrxManager *trx_manager = GCTX.trx_manager_;
  ASSERT(trx_manager != nullptr, "cannot do recover that trx_manager is null");

  // 恢复时，LogEntryIterator读取到的可能还有一些未提交的事务的日志，但它们不应该被重做
  // 因此，需要记录下来哪些事务尚未提交
  std::unordered_set<int32_t> uncommitted_trx_ids;

  // LogEntryIterator 类是 Redo 日志文件的读取工具，每次读取一条日志项
  // 使用这个类读取redo.log，从头开始遍历日志文件，直到读到文件尾
  auto it = LogEntryIterator();
  it.init(*log_file_);
  for (it.next(); it.valid(); it.next()) {
    // 对于读出来的每一条日志项，根据日志项的不同类型执行不同的恢复操作
    const auto &log_entry = it.log_entry();
    switch (log_entry.log_type()) {
      case LogEntryType::MTR_BEGIN: {
        // 如果是事务开始的日志项，则要根据日志项中的事务id开启一个事务
        // 提示：调用Trx *MvccTrxManager::create_trx(int32_t trx_id)
        trx_manager->create_trx(log_entry.trx_id());
        // 添加到未提交事务的集合中
        uncommitted_trx_ids.insert(log_entry.trx_id());
        break;
      }
      case LogEntryType::MTR_COMMIT: {
        // 根据日志项中的事务id找到它所属的事务对象，然后由该事务对象进行重做操作
        // 提示：调用RC MvccTrx::redo(Db *db, const LogEntry &log_entry)
        trx_manager->find_trx(log_entry.trx_id())->redo(db, log_entry);
        // 从未提交事务的集合中移除
        uncommitted_trx_ids.erase(log_entry.trx_id());
        break;
      }
      // 忽略 ERROR操作
      case LogEntryType::ERROR: {
        break;
      }
      default:
        // 重做其他操作
        trx_manager->find_trx(log_entry.trx_id())->redo(db, log_entry);
        break;
    }
  }

  // rollback 未提交的事务
  for (const auto &trx_id : uncommitted_trx_ids) {
    trx_manager->find_trx(trx_id)->rollback();
  }

  return RC::SUCCESS;
}

