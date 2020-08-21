lexer grammar CommonLexerRules;
CHECKPOINT:'checkpoint' | 'CHECKPOINT';
CONF : 'conf' | 'CONF';
ZK : 'zk' | 'ZK';
ROWKEY : 'rowkey' | 'ROWKEY';
JOIN: 'join'| 'JOIN';
SELECT : 'select'| 'SELECT';
FROM : 'from'| 'FROM';
ON : 'on'| 'ON' ;
WHERE : 'WHERE'|'where';
PRT: 'PRINT'| 'print';
INTO: 'into' | 'INTO';
// 定义关键词的
WS : [ \t\r\n]+ -> skip;
IDENTIFIER
        : (LETTER | DIGIT | '_' | ':')+
        | '`' (LETTER | DIGIT | '_' | ':')+ '`'
        ;
fragment LETTER
    : [A-Za-z]
    ;
// 关键词必须以大写开头
FLOAT : DIGIT+ '.' DIGIT*    // 1.39、3.14159等
      | '.' DIGIT+           // .12 (表示0.12)
      ;
NUMBER : [1-9][0-9]*|[0]|([0-9]+[.][0-9]+) ;
fragment DIGIT : [0-9];    // 匹配单个数字
// 单行注释(以//开头，换行结束)
LINE_COMMENT : '//' .*? '\r'?'\n' -> skip;
// 多行注释(/* */包裹的所有字符)
COMMENT : '/*' .*? '*/' -> skip;

MUL : '*';
ADD : '+';
INT : '0' | [1-9][0-9]*;
NEWLINE : '\r'?'\n';
STRING : '"' .*? '"'
| '\'' .*? '\'';


