grammar CustomSqlParser;      // 定义一个名为Hello的语法，名字与文件名一致
import CommonLexerRules;

// #是别名
customcal
: helloWordStatement EOF   #helloWord
| checkpointStatement EOF #checkpoint
| hBaseSearchState EOF #selectHbase
| hbaseJoinState EOF #hbaseJoin
| udfFunctionStateTest EOF #udfstate
;

// helloWordStatement
helloWordStatement
  : PRT word=STRING      // 定义一个r规则，匹配一个关键字Hello和紧随其后的标识符ID
  ;     // "|"是备选分支的分隔符

// chckpoint 表数据到某hdfs路径。将spark里面的 view表存到hdfs
checkpointStatement
:CHECKPOINT table=tableNameDefineState INTO location=STRING
;



// 有多个 familyColumns 。  familyColumns里面的 familys 由多个 hBaseFamilyState
hBaseSearchState
:SELECT familyColumns+=hBaseFamilyState (',' familyColumns+=hBaseFamilyState)* FROM tableName=IDENTIFIER  WHERE 'key=' key=STRING
;
// hBaseColumnFamilyState 是由 (  N个   columnDefineState 组成 ) ; columnDefineState 是由 多个 columnDefineState
// columnDefineState 由零个或多个’,’隔开field序列列表
// +=  +表示匹配一次或多次，  = 表示赋值
// info (name1 string, name2 string)
hBaseFamilyState
    : familyName=IDENTIFIER '(' columns+=columnAndTypeDefineState (',' columns+=columnAndTypeDefineState)* ')'
    ;




// 这里要携程 (',' cols+=hbaseJoincolumn) 而不能是 (',' hbaseJoincolumn)，否则cols拿不到完整的
hbaseJoinState
: (tb=createTableDefineState)? SELECT cols+=columnUdfState (',' cols+=columnUdfState)* FROM tablename=tableNameDefineState JOIN hbasetable=tableNameDefineState ON
 ROWKEY '=' joinkey=columnUdfState  CONF ZK '=' zk=STRING;

// udf test
udfFunctionStateTest:
SELECT cols+=columnUdfState (',' cols+=columnUdfState)* FROM tablename=tableNameDefineState
;


// -----------------------------------------------------------------------
// udf方法定义
columnUdfState:
| udfname=IDENTIFIER '(' (paramcols+=columnDefineState (',' paramcols+=columnDefineState)*)? ')' AS newColsName=columnDefineState
| cols=columnDefineState
;


// 字段和字段类型定义；用于建表
columnAndTypeDefineState
    : colName=IDENTIFIER colType=IDENTIFIER
    ;
    
// 字段定义  
columnDefineState: (family=IDENTIFIER '.')?  colname=IDENTIFIER
;

// 建表前缀
createTableDefineState:
CREATE OR REPLACE TEMPORARY VIEW newtablename=tableNameDefineState AS;

// 表名定义
tableNameDefineState
    : (db=IDENTIFIER '.')? table=IDENTIFIER
    | (db=IDENTIFIER ':')? table=IDENTIFIER
    ;

