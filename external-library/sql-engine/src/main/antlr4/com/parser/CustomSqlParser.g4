grammar CustomSqlParser;      // 定义一个名为Hello的语法，名字与文件名一致
import CommonLexerRules;


statment
: query #statementDefault
| CHECKPOINT table=tableNameDefineState INTO location=STRING #checkpoint
| paramName=IDENTIFIER '=' tableName=IDENTIFIER '.' actionName=IDENTIFIER #collectState
;

// #是别名
query:(tb=createTableDefineState)?
 simpleQueryState
 (((where)? (join)?) | ((join)? (where)?))?
 (conf)? #simpleQuery
;

// hBaseColumnFamilyState 是由 (  N个   columnDefineState 组成 ) ; columnDefineState 是由 多个 columnDefineState
// columnDefineState 由零个或多个’,’隔开field序列列表
// +=  +表示匹配一次或多次，  = 表示赋值
// info (name1 string, name2 string)
//hBaseFamilyState
//    : familyName=IDENTIFIER '(' columns+=columnAndTypeDefineState (',' columns+=columnAndTypeDefineState)* ')'
//    ;
//

// 这里要 (',' cols+=hbaseJoincolumn) 而不能是 (',' hbaseJoincolumn)，否则cols拿不到完整的

// where 查询条件
where:
WHERE whereExpression=booleanExpression
;


// join表达式
join:
JOIN rightTable=tableNameDefineState ON joinExpression
;

// 配置信息
conf
:CONF confParam+=keyValue (',' confParam+=keyValue)*
;
// key value
keyValue:
key=IDENTIFIER '=' value=STRING
;

// a=b a>b a<b
valueExpression
    : columnUdfState | STRING
    | left=valueExpression comparisonOperator right=valueExpression
    ;
// join的on后面的表达式
joinExpression
  : columnUdfState
  | left=joinExpression '=' right=joinExpression
  | left=joinExpression operator=AND right=joinExpression
  ;
// a=b and a>c
booleanExpression
    : NOT booleanExpression    #logicalNot
    | valueExpression                                 #predicated
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

comparisonOperator
:operator= ('=' | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ | EQ)
;


// udf test
simpleQueryState:
SELECT cols+=columnUdfState (',' cols+=columnUdfState)* FROM sourceTable=tableNameDefineState
;


// -----------------------------------------------------------------------
// udf方法定义. 支持 udf多嵌套 . 之后支持int和Double类型
columnUdfState:
 udfname=IDENTIFIER '(' (paramcols+=columnUdfState (',' paramcols+=columnUdfState)*)? ')' asName?
| col=columnDefineState asName?
| constantParam=(STRING|FLOAT) asName?
;

asName:
AS asColName=columnDefineState
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
CREATE OR REPLACE TEMPORARY VIEW createTablename=tableNameDefineState AS;
// 表名定义,设置别名
tableNameDefineState
    :(db=IDENTIFIER DBSEPARATOR)? table=IDENTIFIER (aliasName=IDENTIFIER)?
    ;
