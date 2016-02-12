# anomaly-detection
python scripts for anomaly-detection

# How to run
- Please set environ variable ADMIN_QUESTIONS_WRITE_TOKEN in .profile 
- If using supervisor, remember to `supervisorctl reread` and `supervisor update`

1. Make sure Redis is running locally
2. `pip install -r requirements.txt`
3. `python run.py configs/whatever.yml`
