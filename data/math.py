import numpy as np


def _sma(data):
    length = len(data)
    return data[0] if length == 1 else round(sum(data) / length, 2)


def _ema(arr):
    length = len(arr)
    weight = 2 / (length + 1)

    data = np.zeros(length)
    for i in range(length):
        data[i] = arr[i] if i == 0 else weight * arr[i] + (1 - weight) * data[i - 1]  # 从首开始循环

    return data[-1]


print (_sma([1]))
