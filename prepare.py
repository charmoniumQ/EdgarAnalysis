import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score
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
    return np.sum((Y1 - Y2)**2) / len(Y1)

def position(predY, threshold=1, weight=True):
    if weight:
        # invest in stocks however much Y tells you to
        p = predY.copy()
    else:
        # invest in all equally to start with
        p = np.ones_like(predY, dtype=np.float64)

    # but drop ones less than thresh
    p[predY < threshold] = 0
    # normalize by the sum (to get a percent)
    p /= np.sum(p)
    return p

def benchmark(Y):
    return Y / len(Y)

def returns(pos, Y):
    return pos * Y

def sharpe(pos, Y):
    # https://en.wikipedia.org/wiki/Sharpe_ratio
    delta = returns(pos, Y) - benchmark(Y)
    return np.sum(delta) / np.std(delta)

def debug():
    np.set_printoptions(precision=2, linewidth=150, suppress=True)
    year = None
    train = 1000
    test = 1000

    data = retrieve_feature_data(train + test, year)
    print("Using {} / {} datapoints".format(train + test, len(data)))

    X, Y = to_matrix(data)
    trainX, trainY = X[train:], Y[train:]
    testX, testY = X[:train], Y[:train]
    # print('data = \n{!s}'.format(np.hstack((trainX, trainY))))
    # print('max = {} at {}'.format(trainX.max(), trainX.argmax()))
    reg = LinearRegression()
    reg.fit(trainX, trainY)
    predY = reg.predict(testX)
    # print('error = \n{!s}'.format(np.hstack((predY, testY, (predY - testY)**2))))
    print('Coefficients (x1e3) = {!s}'.format(reg.coef_ * 1e3))

    print('Mean Squared Error = {:.4f}'.format(mse(predY, testY)))
    pos = position(predY, threshold=1)
    print('Prediction, actual, benchmark, position, return = \n{!s}'.format(np.hstack((predY, testY, benchmark(testY), pos, returns(pos, testY)))))
    print('Returns = {:.0f}%, Adjusted returns = {:.0f}%, with a Sharpe ratio of {:.2f}'
          .format((np.sum(returns(pos, testY)) - 1)* 100,
                  np.sum(returns(pos, testY) - benchmark(testY)) * 100,
                  sharpe(pos, testY)))

import matplotlib.pyplot as plt

def convergence(minN, maxN, spaceN, testN, mult):
    ns = np.arange(minN, maxN, spaceN)
    plt.figure()
    plt.xlabel('Number of data points')
    plt.title('Convergence of Ordinary Least Squares Regrssion')

    data = retrieve_feature_data(maxN + testN, None)
    X, Y = to_matrix(data)
    testX, testY = X[-testN:], Y[-testN:]
    params = np.zeros_like(ns, dtype=np.float64)
    r2 = np.zeros_like(ns, dtype=np.float64)
    m = np.zeros_like(ns, dtype=np.float64)
    reg = LinearRegression()
    final = reg.fit(X, Y).coef_
    for i, n in enumerate(ns):
        reg.fit(X[:n], Y[:n])
        params[i] = np.sum(((reg.coef_ - final) * mult)**2)
        r2[i] = reg.score(testX, testY)
        m[i] = mse(reg.predict(testX), testY)
    plt.plot(ns, params / params.max(), 'g', label=r'$\| \theta - \hat{\theta} \|$')
    plt.plot(ns, -np.arctan(r2) / np.pi * 2 , 'r', label='$r^2$')
    plt.plot(ns, m / m.max(), 'y', label=r'$\mathbb{E} \left[ (f_\theta(X) - \hat{Y})^2 \right]$')
    plt.gca().set_yticklabels([])
    plt.legend()
    plt.savefig('convergence.png')
    plt.close()

def thresh_calc(maxT, nT, N=4000, M=1000):
    plt.figure()
    plt.xlabel('Threshold value')
    plt.ylabel('Sharpe ratio')
    X, Y = to_matrix(retrieve_feature_data(N, None))
    trainX, trainY = X[:M], Y[:M]
    testX, testY = X[M:], Y[M:]
    reg = LinearRegression()
    reg.fit(trainX, trainY)
    ts = np.linspace(1, maxT, nT)
    sharpe_w = np.zeros_like(ts)
    sharpe_nw = np.zeros_like(ts)
    for i, t in enumerate(ts):
        sharpe_w[i] = sharpe(position(reg.predict(testX), t, True), testY)
        sharpe_nw[i] = sharpe(position(reg.predict(testX), t, False), testY)
    plt.plot(ts, sharpe_w, 'g', label='Proportional weighting')
    # plt.plot(ts, sharpe_nw, 'b', label='Equal weighting')
    print(testY.max())
    print(sharpe_w)
    print(sharpe_nw)
    plt.legend()
    plt.savefig('thresh.png')
    plt.close()

def cross_val(N=4000, k=60):
    X, Y = to_matrix(retrieve_feature_data(N, None))
    reg = LinearRegression()

    def my_cv(scoring):
        return cross_val_score(reg, X, Y, cv=k, scoring=scoring)

    MSE = my_cv(lambda f, X, Y: mse(f.predict(X), Y))
    plt.figure()
    plt.title('Mean squared error in multiple trials')
    plt.ylabel('Frequency')
    plt.xlabel('MSE')
    print(np.mean(MSE), np.median(MSE))
    plt.hist(MSE, bins=7, normed=True)
    plt.savefig('mse.png')
    plt.close()

    r = my_cv(lambda f, X, Y: 100 * (np.sum(returns(position(f.predict(X)), Y)) - 1))
    plt.figure()
    plt.title('Returns in multiple trials')
    plt.ylabel('Frequency')
    plt.xlabel('Returns (%)')
    print(np.mean(r), np.median(r))
    plt.hist(r, bins=10, normed=True)
    plt.savefig('returns.png')
    plt.close()

    s = my_cv(lambda f, X, Y: sharpe(position(f.predict(X)), Y))
    plt.figure()
    plt.title('Sharpe ratio in multiple trials')
    plt.ylabel('Frequency')
    plt.xlabel('Sharpe')
    print(np.mean(s), np.median(s))
    plt.hist(s, bins=10, normed=True)
    plt.savefig('sharpe.png')
    plt.close()

if __name__ == '__main__':
    convergence(minN=50, maxN=1500, spaceN=10, mult=3e1, testN=2000)
    # thresh_calc(1.05, 10, N=4000)
    # cross_val()
