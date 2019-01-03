grammar Metatron;
exprs:	(stmt TERMINATOR)* | stmt ;

stmt: 'describe' RESOURCE ;

RESOURCE : [A-Za-z0-9_-]+ ;

TERMINATOR : ';' ;

WS: [ \n\t\r]+ -> skip;
IDENTIFIER : [a-zA-Z0-9_-]+;