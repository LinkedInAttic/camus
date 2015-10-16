package com.linkedin.camus.etl;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Listener interface to perform actions when messages are read/write and after they get processed.<br>
 * <p>
 * Use this interface to add functionality to the processing pipeline. Note the implementations are meant to be fairly
 * simple and do small per-message processing.
 * </p>
 * <p>
 * Listeners of this type are completely optional and can be configured per record reader and record writer for each
 * topic. Out of the box, no listener are provided or pre-set.
 * </p>
 * <p>
 * An optional, global, default implementation can specified as:<br>
 * <b>etl.record.reader.listener</b>=com.company.somepackage.SomeDefaultReaderImplementation<br>
 * <b>etl.record.writer.listener</b>=com.company.somepackage.SomeDefaultWriterImplementation<br>
 * </p>
 * <p>
 * Per-topic listeners a configured as:<br>
 * <b>etl.record.reader.listener.<i>topic-name</i></b>=com.company.somepackage.TopicSpecifictReaderImplementation<br>
 * <b>etl.record.writer.listener.<i>topic-name</i></b>=com.company.somepackage.TopicSpecifictWriterImplementation<br>
 * </p>
 */
public interface EtlRecordListener {

  /**
   * This is called once the messages is ready to be processed.
   *
   * @param context Hadoop task context
   * @param key ETL message key
   */
  void onNew(TaskAttemptContext context, IEtlKey key);

  /**
   * This method is invoked once a successful read/write is performed.
   *
   * @param context Hadoop task context
   * @param key ETL message key
   */
  void onSuccess(TaskAttemptContext context, IEtlKey key);

  /**
   * This method is invoked when an error occurs during the messages processed, right between the invocation of
   * {@link onBefore(TaskAttemptContext, IEtlKey)} and the read/write operation.
   *
   * @param context Hadoop task context
   * @param key ETL message key
   * @param e optional Exception that caused the error - can be {@code null}
   */
  void onFailure(TaskAttemptContext context, IEtlKey key, Exception e);

}
