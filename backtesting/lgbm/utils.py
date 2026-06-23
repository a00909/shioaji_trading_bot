from pathlib import Path

import numpy as np
from lightgbm import Booster


def get_model(root: str | Path = None):
    if root:
        root = Path(root).resolve()
    else:
        root = Path(__file__).resolve().parent / "model.txt"

    return Booster(model_file=root)


def thresh_chooser(arr):
    perc_list = [25, 50, 75, 99.0, 99.5, 99.95]
    threshs = [np.percentile(arr, perc) for perc in perc_list]
    samples = [np.sum(arr >= thresh) for thresh in threshs]
    smp_cnt = [(p, float(t), int(s)) for p, t, s in zip(perc_list, threshs, samples)]
    print('idx\tperc\tthresh\tcount')
    for idx, (p, t, s) in enumerate(smp_cnt):
        print(f'{idx}\t{p}\t{t}\t{s}')

    choose = int(input('choose thresh: '))
    thresh = threshs[choose]
    return thresh