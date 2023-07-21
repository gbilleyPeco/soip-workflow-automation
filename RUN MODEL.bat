call C:\Users\gbilley\Anaconda3\condabin\activate.bat
::call conda env create -f environment.yml
::call conda env update -f environment.yml
call conda activate soip-workflow-automation
python %~dp0src\01-soip-model-update-process.py
call conda deactivate
pause