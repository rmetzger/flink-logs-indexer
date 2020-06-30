package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;

import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;

public class LogsIndexerTest {

    @Test
    public void testParseExceptions() throws IOException {
        String sampleText = "12:52:27,904 [                main] INFO  org.apache.flink.streaming.runtime.tasks.StreamTask          [] - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)\n" +
                "12:52:27,908 [                main] WARN  org.apache.flink.streaming.api.operators.BackendRestorerProcedure [] - Exception while restoring keyed state backend for TestOneInputStreamOperator_a53aa6a643bbc25bfe21a4e3b6141eaa_(1/1) from alternative (1/2), will retry while more alternatives are available.\n" +
                "org.apache.flink.runtime.state.BackendBuildingException: Failed when trying to restore heap backend\n" +
                "\tat org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder.build(HeapKeyedStateBackendBuilder.java:116) ~[flink-runtime_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.runtime.state.filesystem.FsStateBackend.createKeyedStateBackend(FsStateBackend.java:540) ~[flink-runtime_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl.lambda$keyedStatedBackend$1(StreamTaskStateInitializerImpl.java:301) ~[flink-streaming-java_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.api.operators.BackendRestorerProcedure.attemptCreateAndRestore(BackendRestorerProcedure.java:142) ~[flink-streaming-java_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.api.operators.BackendRestorerProcedure.createAndRestore(BackendRestorerProcedure.java:121) [flink-streaming-java_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl.keyedStatedBackend(StreamTaskStateInitializerImpl.java:317) [flink-streaming-java_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl.streamOperatorStateContext(StreamTaskStateInitializerImpl.java:144) [flink-streaming-java_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.api.operators.AbstractStreamOperator.initializeState(AbstractStreamOperator.java:247) [flink-streaming-java_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness.initializeState(AbstractStreamOperatorTestHarness.java:505) [flink-streaming-java_2.11-1.12-SNAPSHOT-tests.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.test.state.operator.restore.StreamOperatorSnapshotRestoreTest.testOperatorStatesSnapshotRestoreInternal(StreamOperatorSnapshotRestoreTest.java:267) [test-classes/:?]\n" +
                "\tat org.apache.flink.test.state.operator.restore.StreamOperatorSnapshotRestoreTest.testOperatorStatesSnapshotRestoreWithLocalStateDeletedTM(StreamOperatorSnapshotRestoreTest.java:156) [test-classes/:?]\n" +
                "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_242]\n" +
                "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_242]\n" +
                "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_242]\n" +
                "\tat java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_242]\n" +
                "\tat org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.rules.TestWatcher$1.evaluate(TestWatcher.java:55) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.rules.RunRules.evaluate(RunRules.java:20) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner$3.run(ParentRunner.java:290) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.access$000(ParentRunner.java:58) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.run(ParentRunner.java:363) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.Suite.runChild(Suite.java:128) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.Suite.runChild(Suite.java:27) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner$3.run(ParentRunner.java:290) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.access$000(ParentRunner.java:58) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27) [junit-4.12.jar:4.12]\n" +
                "\tat org.junit.runners.ParentRunner.run(ParentRunner.java:363) [junit-4.12.jar:4.12]\n" +
                "\tat org.apache.maven.surefire.junit4.JUnit4Provider.execute(JUnit4Provider.java:365) [surefire-junit4-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.junit4.JUnit4Provider.executeWithRerun(JUnit4Provider.java:273) [surefire-junit4-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.junit4.JUnit4Provider.executeTestSet(JUnit4Provider.java:238) [surefire-junit4-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.junit4.JUnit4Provider.invoke(JUnit4Provider.java:159) [surefire-junit4-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.booter.ForkedBooter.invokeProviderInSameClassLoader(ForkedBooter.java:384) [surefire-booter-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.booter.ForkedBooter.runSuitesInProcess(ForkedBooter.java:345) [surefire-booter-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.booter.ForkedBooter.execute(ForkedBooter.java:126) [surefire-booter-2.22.1.jar:2.22.1]\n" +
                "\tat org.apache.maven.surefire.booter.ForkedBooter.main(ForkedBooter.java:418) [surefire-booter-2.22.1.jar:2.22.1]\n" +
                "Caused by: java.io.FileNotFoundException: /tmp/junit5590154506259923007/junit5872868844877881211/jid_d4968cf562a8633b15941212e26a6022/vtx_8db8ae272c2c54a7122d4f733250c7e9_sti_0/chk_1/101d0500-2171-413e-b61d-cffb65c3b848 (No such file or directory)\n" +
                "\tat java.io.FileInputStream.open0(Native Method) ~[?:1.8.0_242]\n" +
                "\tat java.io.FileInputStream.open(FileInputStream.java:195) ~[?:1.8.0_242]\n" +
                "\tat java.io.FileInputStream.<init>(FileInputStream.java:138) ~[?:1.8.0_242]\n" +
                "\tat org.apache.flink.core.fs.local.LocalDataInputStream.<init>(LocalDataInputStream.java:50) ~[flink-core-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.core.fs.local.LocalFileSystem.open(LocalFileSystem.java:142) ~[flink-core-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.runtime.state.filesystem.FileStateHandle.openInputStream(FileStateHandle.java:69) ~[flink-runtime_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.runtime.state.KeyGroupsStateHandle.openInputStream(KeyGroupsStateHandle.java:118) ~[flink-runtime_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.runtime.state.heap.HeapRestoreOperation.restore(HeapRestoreOperation.java:124) ~[flink-runtime_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\tat org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder.build(HeapKeyedStateBackendBuilder.java:114) ~[flink-runtime_2.11-1.12-SNAPSHOT.jar:1.12-SNAPSHOT]\n" +
                "\t... 47 more\n" +
                "12:52:27,922 [                main] INFO  org.apache.flink.runtime.state.heap.HeapKeyedStateBackend    [] - Initializing heap keyed state backend with stream factory.\n" +
                "12:52:27,923 [                main] INFO  org.apache.flink.runtime.io.disk.FileChannelManagerImpl      [] - FileChannelManager removed spill file directory /tmp/flink-io-84dbfac7-6961-499d-98f3-014f3ce49fc1";

        TestLogsIndexer indexer = new TestLogsIndexer();
        InputStream targetStream = new ByteArrayInputStream(sampleText.getBytes());
        indexer.parseLogfile(targetStream, "logs-ci-blinkplanner/20200629.4.tar.gz", "logs-ci-blinkplanner/20200629.4.tar.gz");
        Assert.assertEquals(4, indexer.getEmittedLogs().size());
        Assert.assertTrue(indexer.getEmittedLogs().get(1).contains("org.apache.flink.test.state.operator.restore.StreamOperatorSnapshotRestoreTest.testOperatorStatesSnapshotRestoreWithLocalStateDeletedTM"));
        MatcherAssert.assertThat(indexer.getEmittedLogs().get(2), containsString("12:52:27,922") );
        Assert.assertTrue(indexer.getEmittedLogs().get(1).contains("12:52:27,908"));
    }

    private static class TestLogsIndexer extends LogsIndexer {
        public List<String> emittedLogs = new ArrayList<>();
        public TestLogsIndexer() {
            super(ParameterTool.fromMap(new HashMap<>()));
        }

        @Override
        protected void emitLogToElastic(String line, String buildname, long timestamp) throws IOException {
            emittedLogs.add(line);
        }

        public List<String> getEmittedLogs() {
            return emittedLogs;
        }
    }

}
