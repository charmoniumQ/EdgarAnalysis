import numpy as np
from sklearn import linear_model
from features import retrieve_feature_data

def s(x):
    '''Maps [0, 1] to [0, inf]'''
    return np.tan(x * np.pi / 2)

def to_matrix(data):
    X = np.zeros((len(data), 7))
    Y = np.zeros((len(data), 1))
    for i, datum in enumerate(data):
        X[i][0] = s(datum['anger'])
        X[i][1] = s(datum['disgust'])
        X[i][2] = s(datum['fear'])
        X[i][3] = s(datum['joy'])
        X[i][4] = s(datum['sadness'])
        X[i][5] = s(datum['sentiment'])
        X[1][6] = datum['sentiment_type']
        Y[i][0] = datum['stock_improvement']
    return X, Y

def mse(Y1, Y2):
    return np.sum((Y1 - Y2)**2) / len(X)

def position(predY, threshold):
    # invest in stocks however much Y tells you to
    p = predY.copy()
    # but drop ones less than thresh
    p[predY < threshold] = 0
    # normalize by the sum (to get a percent)
    p /= len(p)
    return p

def benchmark(Y):
    return Y / len(Y)

def returns(pos, Y):
    return pos * Y

def sharpe(pos, Y):
    # https://en.wikipedia.org/wiki/Sharpe_ratio
    delta = returns(pos, Y) - benchmark(Y)
    return np.sum(delta) / np.std(delta)

if __name__ == '__main__':
    year = 2016
    train = 500
    test = 500
    np.set_printoptions(precision=2, linewidth=150, suppress=True)

    X, Y = to_matrix(retrieve_feature_data(train + test, year))
    trainX, trainY = X[train:], Y[train:]
    testX, testY = X[train:], Y[train:]
    # print('data = \n{!s}'.format(np.hstack((trainX, trainY))))
    print('max = {} at {}'.format(trainX.max(), trainX.argmax()))
    reg = linear_model.LinearRegression()
    reg.fit(trainX, trainY)
    predY = reg.predict(testX)
    # print('error = \n{!s}'.format(np.hstack((predY, testY, (predY - testY)**2))))
    print('Coefficients = {!s}'.format(reg.coef_))

    print('Mean Squared Error = {:.2f}'.format(mse(predY, testY)))
    # pos = position(predY, threshold=0)
    # print('Prediction, position, actual, return = \n{!s}'.format(np.hstack((predY, pos, testY, returns(pos, testY)))))
    # print('Return = {:.0f}%, with a Sharpe ratio of {:.2f}'
    #       .format(np.sum(returns(pos, testY)) * 100, sharpe(pos, testY)))
