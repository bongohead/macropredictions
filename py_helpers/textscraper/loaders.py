import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm
import numpy as np
import yaml
import pandas as pd

def load_config(f: str, prompt_id: str, label_key: str):
    """
    Loads the YAML config files containing label mappings and examples.

    Params:
        @f: The filepath of the YAML config.
        @prompt_id: The prompt ID of the YAML field.
        @label_key: The label key to classify.
        
    Returns:
        A tuple with a data frame of label <-> integer mappings and a list of dictionaries corresponding to examples.
    """
    with open(f, 'r') as file:
        yaml_dict = [x for x in yaml.safe_load(file)[prompt_id] if x['model'] == label_key][0]
        
    return (pd.DataFrame.from_dict(yaml_dict['map']), yaml_dict['examples'])

class TextDataset(Dataset):
    """
    Dataset class for multiclass labeling

    Params:
        @tokenizer: The tokenizer used to convert text into tokens.
        @texts: A list of strings, each representing a unique text sample.
        @labels A list of labels, corresponding to a label for each string in texts. Pass None when not available (e.g. for ifnerence)
    """
    def __init__(self, tokenizer, texts: list[str], labels = None):
        self.tokenizer = tokenizer
        self.texts = texts
        self.inputs = self.tokenize_and_encode()
        
        if labels is not None:
            self.labels = torch.tensor(labels, dtype = torch.long)
        else:
            self.labels = None
        
    def __len__(self):
        return len(self.texts)
        
    def tokenize_and_encode(self):
        return self.tokenizer(
            self.texts,
            add_special_tokens = True,
            max_length = 512,
            truncation = True,
            padding = 'max_length',
            return_attention_mask = True,
            return_tensors = 'pt'
        )
    
    def __getitem__(self, idx):
        item = {key: vals[idx] for key, vals in self.inputs.items()}
        if self.labels is not None:
            item['labels'] = self.labels[idx]
        return item


@torch.no_grad()
def eval_performance(model, ds: TextDataset, device, batch_size: int = 16, verbose = True) -> dict:
    """
    Run inference on a dataset set and return performance stats.

    Params:
        @model: The model to use.
        @ds: A TextDataset object which contains the data to evaluate on.
        @device: The device to run inference on.
        @batch_size: The batch size for evaluation.
        @verbose: Whether to echo the process
    """
    init_train = model.training
    if init_train:
        model.eval()
    
    total_obs = 0
    total_correct = 0
    nlls = []

    dl = DataLoader(ds, batch_size = batch_size, shuffle = True)
    
    for step, b in tqdm(enumerate(dl), disable = not verbose):
        outputs = model(b['input_ids'].to(device), b['attention_mask'].to(device))
        logits = outputs['logits'].cpu()
        label_ids = b['labels'].cpu()
        
        total_obs += len(label_ids)
        total_correct += np.sum(np.where(np.argmax(logits, axis = 1) == label_ids, 1, 0))
        nlls.append(F.cross_entropy(logits, label_ids))
    
    if init_train:
        model.train()
        
    return {'mean_nll': np.mean(nlls), 'accuracy': total_correct/total_obs, 'count': total_obs}

@torch.no_grad()
def eval_performance_as_str(model, ds: TextDataset, device) -> str:
    """
    Run inference on a dataset and returns a printable string, one per dataset element. Useful for running a small set of test examples. 

    Params:
        @model: The model to use.
        @ds: A TextDataset object which contains the data to evaluate on.
        @device: The device to run inference on.
    """
    init_train = model.training
    if init_train:
        model.eval()
        
    total_correct = 0
    str = ''
    
    dl = DataLoader(ds, batch_size = 1, shuffle = False)
    
    for step, b in enumerate(dl):
        outputs = model(b['input_ids'].to(device), b['attention_mask'].to(device))
        softmax = F.softmax(outputs['logits'].detach().cpu().flatten(), dim = 0)
        label = b['labels'].cpu()

        is_correct = 1 if np.argmax(softmax) == label else 0
        total_correct = total_correct + is_correct
        str += (f'\n{"✅" if is_correct == 1 else "❌"} {softmax[label].numpy().round(2)} - {ds.texts[step]}')
    
    if init_train:
        model.train()

    return f"Correct: {total_correct}/{len(ds)}" + str
    