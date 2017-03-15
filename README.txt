*******************
*  SEARCH ENGINE  *
*******************

INSTRUCTION
-----------
1. Download zip file and extract it.
2. Copy your input files into ‘./input/’ directory which you will find 
   inside extracted folder.
3. Set r+w+x permissions for ‘run.sh’ (i.e. chmod 777 run.sh).
4. Execute ‘run.sh’ file. (Note: You may have to execute doc2unix command 
   on shell script file before executing it.)

Note: You can change any parameter set in shell script file and accordingly 
      you will get the results.


MODIFICATION
------------
I have added few input preprocessing steps such as converting all terms 
into lower cases, stripping all white spaces around terms.
So, you may get somewhat different but improved results i.e. TF-IDF scores.

These modifications improved Search Engine results significantly.
e.g.
1. Old Results:
alice29.txt	3.8495667161360236
asyoulik.txt	1.5171331879477519

2. New Results:
alice29.txt	5.144186085909319
asyoulik.txt	1.8222160124410003


MAINTAINER
----------

Name        Chetan Borse
EMail ID    cborse@uncc.edu
LinkedIn    https://www.linkedin.com/in/chetanrborse

