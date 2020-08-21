grammar CustomSqlParser;      // 定义一个名为Hello的语法，名字与文件名一致
import CommonLexerRules;

// #是别名
customcal
: helloWordStatement EOF   #helloWord
| checkpointStatement EOF #checkpoint
| hBaseSearchState EOF #selectHbase
| hbaseJoinState EOF #hbaseJoin
;

// helloWordStatement
helloWordStatement
  : PRT word=STRING      // 定义一个r规则，匹配一个关键字Hello和紧随其后的标识符ID
  ;     // "|"是备选分支的分隔符

// chckpoint 表数据到某hdfs路径。将spark里面的 view表存到hdfs
checkpointStatement
:CHECKPOINT table=tableIdentifier INTO location=STRING
;


tableIdentifier
    : (db=IDENTIFIER '.')? table=IDENTIFIER
    | (db=IDENTIFIER ':')? table=IDENTIFIER
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
    : familyName=IDENTIFIER '(' columns+=columnDefineState (',' columns+=columnDefineState)* ')'
    ;

columnDefineState
    : colName=IDENTIFIER colType=IDENTIFIER
    ;


// 这里要携程 (',' cols+=hbaseJoincolumn) 而不能是 (',' hbaseJoincolumn)，否则cols拿不到完整的
hbaseJoinState
: SELECT cols+=hbaseJoincolumn (',' cols+=hbaseJoincolumn)* FROM tablename=tableIdentifier JOIN hbasetable=tableIdentifier ON
 ROWKEY '=' joinkey=hbaseJoincolumn  CONF ZK '=' zk=STRING;

hbaseJoincolumn: (family=IDENTIFIER '.')? colname=IDENTIFIER
;


//
//CHECKPOINT:'checkpoint' | 'CHECKPOINT';
//CONF : 'conf' | 'CONF';
//ZK : 'zk' | 'ZK';
//ROWKEY : 'rowkey' | 'ROWKEY';
//JOIN: 'join'| 'JOIN';
//SELECT : 'select'| 'SELECT';
//FROM : 'from'| 'FROM';
//ON : 'on'| 'ON' ;
//WHERE : 'WHERE'|'where';
//PRT: 'PRINT'| 'print';
//INTO: 'into' | 'INTO';



