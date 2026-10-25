Output "/Users/shivam/Developer/concurrent-task-scheduler/assets/demo.gif"

Set FontSize 13
Set FontFamily "JetBrains Mono"
Set Width 1100
Set Height 640
Set Padding 20
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set LetterSpacing 0

Hide
Type "cd ~/Developer && clear"
Enter
Sleep 400ms
Show

Type "cd concurrent-task-scheduler"
Enter
Sleep 600ms

Type "mvn test -q"
Enter
Sleep 3s

Type "java -cp target/classes com.shivamshahi.scheduler.Main"
Enter
Sleep 5s
