This README document contains instructions for the Optimization model update part of 
the monthly SOIP process.
---------------------------------------------------------------------------------------------------
This codebase is meant to replace a series of Alteryx workflows called "SOIP Model Update 
Process - ###" that perform the following tasks:

	1. Pull data from an existing Cosmic Frog optimization model.
	2. Pull data from PECO's data warehouse.
	3. Read data from multiple Excel files.
	4. Calculate new model parameters based.
	5. Upload the new data to Cosmic Frog.
	6. Run a validation script to compare the changes between the old and new models.
---------------------------------------------------------------------------------------------------
The simplest way to run this process is to double-click the batch files named "01_MODEL_UPDATE.bat"
and "02_MODEL_VALIDATION.bat". A black terminal window will pop up and you will see the code 
executing. Print statements will tell you what is happening. When the program is finished running, 
you will see "Press any key to continue..." At this point, the process is complete and you can 
close the window. Do not run "02_MODEL_VALIDATION.bat" until "01_MODEL_UPDATE.bat" has finished.

This method requires the following to be true:
	1. The "user_inputs.py" file has been updated.
	2. The Excel files in the "data" folder have been updated.
	   
	   NOTE: To update user_inputs.py, right click the file and select "Open with", then choose
	   any text editor (Notepad, Visual Studio Code, IDLE, etc.)

	3. You are connected to PECO's VPN.
	4. You have Anaconda installed in your computer. (https://www.anaconda.com/download)
	5. The first line in the "RUN MODEL.bat" file references YOUR Anaconda install location.
	   
	   NOTE: To check this, right-click on "RUN MODEL.bat" and select "Edit".
	   If you see "gbilley" in the first line, 
		(i.e: "call C:\Users\gbilley\Anaconda3\condabin\activate.bat")
	   then you need to change the path. 
	   	(Most likely, you just need to replace "gbilley" with your PECO username.)
---------------------------------------------------------------------------------------------------
Project Folder Structure

soip-workflow-automation/
	data/
		SCAC to Carrier Type.xlsx
		SOIP - Depot Assignments - [Month].xlsx
		SOIP Optimization Assumptions - [Month].xlsx
		Trans RFQ Rates.xlsx
		old/

	src/
		excel_data_validation.py
		soip_model_update_process.py
		soip_model_update_validation.py
		sql_statements.py

	validation/
		[Date]/ A new folder will be made for each date that the program is run. 
			This folder will contain the results of the validation script. Namely:
			- An excel file for each table that was altered, showing the differences between
			  the old and new values.
			- An excel file showing which tables have different primary key rows between 
			  models.
			- An excel file showing which tables have duplicated primary keys within 
			  one model.
			- An output.log file containing a summary of the validation steps for each 
			  Cosmic Frog table that was altered.

	README.txt
	01_MODEL_UPDATE.bat
	02_MODEL_VALIDATION.bat
	user_inputs.py
	.gitignore   	(This can be ignored by the user)
	environment.yml (This can be ignored by the user)

---------------------------------------------------------------------------------------------------
The "data" Folder

This folder contains four Excel files with input data needed to run this program. This data is not 
stored in PECO's data warehouse, and so has to be stored locally. There is also a folder called "old"
where you can move old excel files from past model runs. That way you can keep the "data" folder
less cluttered.

SCAC to Carrier Type.xlsx
	This file simply denotes various SCAC codes as CPU or Dedicated. All CPU and Dedicated 
	SCAC codes should be included in this sheet, so if new carriers are used this sheet will need
	to be updated manually.

Trans RFQ Rates.xlsx
	This file contains transportation request-for-quote rates, provided by the transportation team.

SOIP - Depot Assignments - [Month].xlsx
	This file is produced monthly as part of the SOIP process. 

SOIP - Optimization Assumptions - [Month].xlsx
	This file is produced monthly as part of the SOIP process. 
---------------------------------------------------------------------------------------------------
The "user_inputs.py" File

To open the user_inputs.py file, you can either double-click or right-click and select "Open with" and 
pick any text editor. 

Here you can update any of the user-input parameters used by the model. If you make any changes, 
press "ctrl+s" to save the file and close it.











