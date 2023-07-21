This README document contains instructions for the Optimization model update part of 
the monthly SOIP process.
---------------------------------------------------------------------------------------------------
The purpose of this codebase is to replace the existing Alteryx workflows that perform 
the following tasks:
	1. Pull data from a number of tables in a given Cosmic Frog optimization model.
	2. Pull data from PECO's data warehouse.
	3. Read data from two Excel files related to optimization assumptions and depot assignments.
	4. Alter the data that was downloaded from Cosmic Frog based on the data from PECO's data 
	   warehouse and the Excel files.
	5. Upload the new data to Cosmic Frog, replacing the old data.
---------------------------------------------------------------------------------------------------
The simplest way to run this process is to double-click the batch file named "RUN MODEL.bat".
A black terminal window will pop up and you will see the code executing. Print statements will
tell you what is happening. When the program is finished running, you will see "Press any key 
to continue..." At this point, the process is complete and you can close the window!

This method requires the following to be true:
	1. The "user_inputs.py" and the SOIP Excel files have been updated.
	   
	   NOTE: To update user_inputs.py, right click the file and select "Open with", then choose
	   any text editor (Notepad, Visual Studio Code, IDLE, etc.)

	2. You are connected to PECO's VPN.
	3. You have Anaconda installed in your computer. (https://www.anaconda.com/download)
	4. The first line in the "RUN MODEL.bat" file references YOUR Anaconda install location.
	   
	   NOTE: To check this, right-click on "RUN MODEL.bat" and select "Edit".
	   If the first line is "call C:\Users\gbilley\Anaconda3\condabin\activate.bat", then you
	   need to change the path. 
	   (Most likely, you just need to replace "gbilley" with your PECO username.)
---------------------------------------------------------------------------------------------------