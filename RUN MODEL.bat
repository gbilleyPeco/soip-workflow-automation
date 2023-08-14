call C:\Users\gbilley\Anaconda3\condabin\activate.bat
::call conda env create -f environment.yml
::call conda env update -f environment.yml
call conda activate soip-workflow-automation
python %~dp0src\soip_model_update_process.py
python %~dp0src\soip_model_update_validation.py
call conda deactivate
pause