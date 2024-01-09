use trino_lb_core::trino_query_plan::QueryPlanEstimation;

#[cfg(doc)]
use crate::trino_client::TrinoClient;

/// This functions takes a query and returns a [`QueryPlanEstimation`] in case the estimation
/// returned by the normal flow (e.g. using [`TrinoClient::query_estimation`]) would wrong
/// or causes an exception on Trino. In case the query is good to go to the normal flow [`None`]
/// is returned.
pub fn query_estimation_workarounds(query: &str) -> Option<QueryPlanEstimation> {
    if query.starts_with("use ") {
        // Work around the following error (Trino 414):
        //
        // trino> explain (format json) use foo;
        // Query 20231128_171001_02127_fenvj failed: not yet implemented: Use{}
        // java.lang.UnsupportedOperationException: not yet implemented: Use{}
        //     at io.trino.sql.SqlFormatter$Formatter.visitNode(SqlFormatter.java:234)
        //     at io.trino.sql.SqlFormatter$Formatter.visitNode(SqlFormatter.java:197)
        //     at io.trino.sql.tree.AstVisitor.visitStatement(AstVisitor.java:87)
        //     at io.trino.sql.tree.AstVisitor.visitUse(AstVisitor.java:167)
        //     at io.trino.sql.tree.Use.accept(Use.java:63)
        //     at io.trino.sql.tree.AstVisitor.process(AstVisitor.java:27)
        //     at io.trino.sql.SqlFormatter.formatSql(SqlFormatter.java:169)
        //     at io.trino.sql.analyzer.QueryExplainer.explainDataDefinition(QueryExplainer.java:226)
        //     at io.trino.sql.analyzer.QueryExplainer.getJsonPlan(QueryExplainer.java:140)
        //     at io.trino.sql.rewrite.ExplainRewrite$Visitor.getQueryPlan(ExplainRewrite.java:149)
        //     at io.trino.sql.rewrite.ExplainRewrite$Visitor.visitExplain(ExplainRewrite.java:130)
        //     at io.trino.sql.rewrite.ExplainRewrite$Visitor.visitExplain(ExplainRewrite.java:75)
        //     at io.trino.sql.tree.Explain.accept(Explain.java:61)
        //     at io.trino.sql.tree.AstVisitor.process(AstVisitor.java:27)
        //     at io.trino.sql.rewrite.ExplainRewrite.rewrite(ExplainRewrite.java:72)
        //     at io.trino.sql.rewrite.StatementRewrite.rewrite(StatementRewrite.java:55)
        //     at io.trino.sql.analyzer.Analyzer.analyze(Analyzer.java:92)
        //     at io.trino.sql.analyzer.Analyzer.analyze(Analyzer.java:86)
        //     at io.trino.execution.SqlQueryExecution.analyze(SqlQueryExecution.java:271)
        //     at io.trino.execution.SqlQueryExecution.<init>(SqlQueryExecution.java:206)
        //     at io.trino.execution.SqlQueryExecution$SqlQueryExecutionFactory.createQueryExecution(SqlQueryExecution.java:845)
        //     at io.trino.dispatcher.LocalDispatchQueryFactory.lambda$createDispatchQuery$0(LocalDispatchQueryFactory.java:154)
        //     at io.trino.$gen.Trino_414____20231128_083553_2.call(Unknown Source)
        //     at com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:131)
        //     at com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:74)
        //     at com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:82)
        //     at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
        //     at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
        //     at java.base/java.lang.Thread.run(Thread.java:840)

        return Some(QueryPlanEstimation::default());
    }

    None
}
