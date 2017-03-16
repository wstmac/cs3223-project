# cs3223-project
For Eclipse setup (also valid for Unix system):

1.Create a new project named ''CS3223" (or whatever name)

2.Right click "CS3223" -> new-> source folder. Name the source folder ''testcases"

3.Copy the contents under ''Component/src'' to  ''CS3223/src''  (You can drag and drop)

4.Copy ''Component/testcases/*.java'' to ''CS3223/testcases" (Also drag and drop)

5.Copy  every file in ''Component/testcases/"  except *.java to  CS3223 (Drag and drop)

6.Right click "CS3223"->"Build Path"->"Configure Build Path"->"Libraries"->"Add External Class Folder",  add "Component/lib/CUP" and "Component/lib/JLex"

7.At this step, you should see no more red underlines in the source code. You are ready to go.
