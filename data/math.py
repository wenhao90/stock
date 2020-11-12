import numpy as np


def get_sma(arr):
    length = len(arr)
    return arr[0] if length == 1 else round(sum(arr) / length, 4)


def get_ema(arr):
    length = len(arr)
    weight = 2 / (length + 1)

    data = np.zeros(length)
    for i in range(length):
        data[i] = arr[i] if i == 0 else weight * arr[i] + (1 - weight) * data[i - 1]  # 从首开始循环

    return data[-1]
