1. 宣告一個類別(名稱: InvalidProgressionSizeException)，繼承Exception，建構子需要一個int 參數errorType，override toString()方法，使得toString() 可以根據errorType印出不同訊息，例如:
* m<1時，印出 "m should be greater than 0"
* 級數超出long型態的範圍時，印出"級數超出long型態的範圍"
* ...
2. 將作業2與作業4的防呆全部改成拋出exception的方式處理
