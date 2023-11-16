from datetime import date
import os

def create_model_save_dir(prompt_id, label_key):
    save_dir = f"saves/{prompt_id}-{label_key}-{date.today().strftime('%Y%m%d')}"
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    return save_dir
