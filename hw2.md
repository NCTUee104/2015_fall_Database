 1. （40%）宣告一類別Progression_學號，具有一個long型態的二維陣列屬性progressionArray，其建構子需要input參數兩個integer (m, n)，建構子內可根據m, n來初始化progressionArray，並且將陣列填入值，填入值的原則如下：

* 奇數列為等差級數，首項公差為該列數，例如：progressionArray[1][1]~ progressionArray[1][n]即為公差為1得等差數列

* 偶數列為等比級數，首項公比為該列數，例如：progressionArray[2][1]~ progressionArray[2][n]即為公比為2得等比數列

2.（10%）在類別「Progression_學號」中製作一method，showRow(int r)，印出第r列的值

3.（10%）在類別「Progression_學號」中製作一method，showCol(int c)，印出第c行的值

4.（10%）在類別「Progression_學號」中製作一method，showArray()，印出整個陣列

5.（10%）在類別「Progression_學號」中製作一method，returnRow(int r)，回傳第r列

6.（10%）在類別「Progression_學號」中製作一method，returnCol(int c)，回傳第c行

7.（10%）在類別「Progression_學號」中製作一method，returnArray()，回傳整個陣列

* 注意：須防呆，例如級數值是否可能超出long型態範圍等問題
