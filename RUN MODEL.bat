call C:\Users\gbilley\AppData\Local\anaconda3\condabin\activate.bat
::call conda env create -f environment.yml
::call conda env update -f environment.yml
call conda activate soip-workflow-automation
cd src
python soip_model_update_process.py
python soip_model_update_validation.py
call conda deactivate
pause