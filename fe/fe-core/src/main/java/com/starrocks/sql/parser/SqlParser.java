// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.OriginStatement;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.StringReader;
import java.util.List;

public class SqlParser {
    public static List<StatementBase> parse(String originSql, long sqlMode) {
        List<String> splitSql = splitSQL(originSql);
        List<StatementBase> statements = Lists.newArrayList();

        for (int idx = 0; idx < splitSql.size(); ++idx) {
            String sql = splitSql.get(idx);
            try {
                StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                StarRocksParser parser = new StarRocksParser(tokenStream);
                StarRocksParser.sqlMode = sqlMode;
                parser.removeErrorListeners();
                parser.addErrorListener(new ErrorHandler());
                StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
                StatementBase statement = (StatementBase) new AstBuilder()
                        .visitSingleStatement(sqlStatements.singleStatement(0));
                statement.setOrigStmt(new OriginStatement(sql, idx));
                statements.add(statement);
            } catch (ParsingException parsingException) {
                StatementBase statement = parseWithOldParser(sql, sqlMode);
                if (statement instanceof QueryStmt
                        || statement instanceof InsertStmt
                        || statement instanceof CreateTableAsSelectStmt
                        || statement instanceof CreateViewStmt
                        || statement instanceof AlterViewStmt
                        || statement instanceof ShowDbStmt
                        || statement instanceof ShowTableStmt) {
                    throw parsingException;
                }
                statements.add(statement);
            }
        }

        return statements;
    }

    private static StatementBase parseWithOldParser(String originStmt, long sqlMode) {
        SqlScanner input = new SqlScanner(new StringReader(originStmt), sqlMode);
        com.starrocks.analysis.SqlParser parser = new com.starrocks.analysis.SqlParser(input);
        try {
            return SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            throw new ParsingException("Please check your sql, we meet an error when parsing.");
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            if (errorMessage == null) {
                throw new ParsingException(e.getMessage());
            } else {
                throw new ParsingException(errorMessage);
            }
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                throw new ParsingException("Internal Error");
            } else {
                throw new ParsingException("Internal Error: " + errorMessage);
            }
        }
    }

    private static List<String> splitSQL(String sql) {
        List<String> sqlLists = Lists.newArrayList();
        boolean inString = false;
        int sqlStartOffset = 0;
        for (int i = 0; i < sql.length(); ++i) {
            if (sql.charAt(i) == '\"' || sql.charAt(i) == '\'' || sql.charAt(i) == '`') {
                inString = !inString;
            }

            if (sql.charAt(i) == ';') {
                if (!inString) {
                    sqlLists.add(sql.substring(sqlStartOffset, i));
                    sqlStartOffset = i + 1;
                }
            }
        }
        sqlLists.add(sql.substring(sqlStartOffset));
        return sqlLists;
    }
}
