import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score, ShuffleSplit
from features import retrieve_feature_data
import matplotlib.pyplot as plt

random_state = 42
power = 3.05

def s(x):
    '''Maps [0, 1] to [0, inf)'''
    return np.tan(x * np.pi / 2)

def to_matrix(data, power):
    '''Loads a list of dicts into a matrix of input (X) and output (Y) suitable for use with scikit learn'''
    X = np.zeros((len(data), 7))
    Y = np.zeros((len(data), 1))
    for i, datum in enumerate(data):
        X[i][0] = s(datum['anger'])
        X[i][1] = s(datum['disgust'])
        X[i][2] = s(datum['fear'])
        X[i][3] = s(datum['joy'])
        X[i][4] = s(datum['sadness'])
        X[i][5] = s((datum['sentiment'] + 1) / 2)
        X[i][6] = datum['sentiment_type']
        Y[i][0] = datum['stock_improvement']**power
    return np.clip(X, -1, 10), Y

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
    if np.sum(p) > 0:
        p = p / np.sum(p)
    return p

def benchmark(Y):
    # Invest the same amount in every stock (buy 1 / len(Y) of each stock)
    # and since the prices go up by a factor of Y
    # you make Y / len(Y) rate of return
    return Y / len(Y)

def returns(pos, Y):
    # pos is how much of each stock you have
    # Y is the percentage of price increase
    return pos * Y

def sharpe(pos, Y):
    # https://en.wikipedia.org/wiki/Sharpe_ratio
    delta = returns(pos, Y) - benchmark(Y)
    return np.sum(delta) / np.std(delta)

def debug():
    year = None
    total = 2000
    train = 1000

    np.set_printoptions(precision=2, linewidth=150, suppress=True)
    data = retrieve_feature_data(total, year)
    assert total == len(data), 'only got {} out of {} datapoints from server'.format(len(data), total)

    print('Random state = {random_state}'.format(**globals()))
    X, Y = to_matrix(data, power)
    trainX, testX, trainY, testY = train_test_split(X, Y, train_size=train/total, random_state=random_state)
    # print('train data = \n{!s}'.format(np.hstack((trainX, trainY))))
    reg = LinearRegression()
    reg.fit(trainX, trainY)

    # predicted price increase factors
    predY = reg.predict(testX)
    # print('test data = \n{!s}'.format(np.hstack((testX, testY, predY))))
    print('Coefficients (x1e3) = {!s}'.format(reg.coef_ * 1e3))

    print('Mean Squared Error = {:.4f}'.format(mse(predY**(1/power), testY**(1/power))))
    pos = position(predY, threshold=1.1)
    print('Taking {} stocks'.format(np.sum(pos > 0)))
    print('Prediction, actual, benchmark, position, return = \n{!s}'.format(np.hstack((predY, testY, benchmark(testY), pos, returns(pos, testY)))))
    print('Return = {:.5f}%, Adjusted return = {:.5f}%, with a Sharpe ratio of {:.2f}'
          .format((np.sum(returns(pos, testY)) - 1) * 100,
                  np.sum(returns(pos, testY) - benchmark(testY)) * 100,
                  sharpe(pos, testY)))
    # plt.figure()
    # plt.hist((returns(pos, testY) - 1)*100)
    # plt.savefig('return_hist.png')
    # plt.close()
    # plt.figure()
    # plt.plot(predY)
    # plt.plot(testY)
    # plt.savefig('actual.png')
    # plt.close()

def convergence(minN, maxN, spaceN, testN, mult):
    ns = np.arange(minN, maxN, spaceN)
    plt.figure()
    plt.xlabel('Number of data points')
    plt.title('Convergence of Ordinary Least Squares Regression')

    data = retrieve_feature_data(maxN + testN, None)
    X, Y = to_matrix(data, power)
    testX, testY = X[-testN:], Y[-testN:]
    params = np.zeros_like(ns, dtype=np.float64)
    r2 = np.zeros_like(ns, dtype=np.float64)
    m = np.zeros_like(ns, dtype=np.float64)
    reg = LinearRegression()
    final = reg.fit(X[:maxN], Y[:maxN]).coef_
    for i, n in enumerate(ns):
        trainX, trainY = X[:n], Y[:n]
        reg.fit(trainX, trainY)
        params[i] = np.sum(((reg.coef_ - final) * mult)**2)
        r2[i] = reg.score(testX, testY)
        m[i] = mse(np.clip(reg.predict(testX), 0, 10)**(1/power), testY**(1/power))
    plt.ylim(0, 1)
    plt.plot(ns, params / params.max() * 2.2, 'g', label=r'Parameter distance')
    plt.plot(ns, -np.arctan(r2) / np.pi * 2, 'r', label='Correlation coefficient')
    plt.plot(ns, m / m.max() * 0.1, 'y', label=r'Mean-squared error')
    plt.gca().set_yticklabels([])
    plt.legend()
    plt.savefig('convergence.png')
    plt.close()

def parameter_tuning(end, step, N=4000, M=1000, k=30):
    reg = LinearRegression()
    powers = np.arange(1, end, step)
    MSE = np.zeros(len(powers), dtype=np.float64)
    sharpe2 = np.zeros(len(powers), dtype=np.float64)
    data = retrieve_feature_data(N, None)
    for i, power in enumerate(powers):
        X, Y = to_matrix(data, power=power)
        def my_cv(scoring):
            # http://scikit-learn.org/stable/modules/cross_validation.html
            return cross_val_score(reg, X, Y, cv=ShuffleSplit(n_splits=k, train_size=M/N, random_state=random_state), scoring=scoring)
        MSE[i] = np.mean(my_cv(lambda f, X, Y: mse(np.clip(f.predict(X), 0, 10)**(1/power), Y**(1/power))))
        sharpe2[i] = np.mean(my_cv(lambda f, X, Y: sharpe(position(f.predict(X), threshold=1.1), Y)))
    print(MSE)
    print(sharpe2)
    plt.figure()
    plt.xlabel('$p$')
    plt.ylim(0, 1)
    plt.plot(powers, MSE / MSE.max(), 'b', label=r'Mean Squared Error')
    plt.plot(powers, sharpe2 / sharpe2.max(), 'y', label='Sharpe Ratio')
    plt.legend()
    plt.savefig('parameter.png')
    plt.close()

def cross_val(N=4000, M=1000, k=60):
    X, Y = to_matrix(retrieve_feature_data(N, None), power)
    reg = LinearRegression()

    def my_cv(scoring):
        # http://scikit-learn.org/stable/modules/cross_validation.html
        return cross_val_score(reg, X, Y, cv=ShuffleSplit(n_splits=k, train_size=M/N, random_state=random_state), scoring=scoring)

    MSE = my_cv(lambda f, X, Y: mse(np.clip(f.predict(X), 0, 10)**(1/power), Y**(1/power)))
    print(np.mean(MSE), np.median(MSE))
    r = my_cv(lambda f, X, Y: 100 * (np.sum(returns(position(f.predict(X)), Y) - benchmark(Y))))
    print(np.mean(r), np.median(r))
    s = my_cv(lambda f, X, Y: sharpe(position(f.predict(X), threshold=1.1), Y))
    print(np.mean(s), np.median(s))
    n = my_cv(lambda f, X, Y: np.sum(f.predict(X) > 0))
    print(np.mean(n), np.median(n))

    plt.figure()
    plt.title('Mean squared error in {k}-fold cross validation'.format(**locals()))
    plt.ylabel('Frequency')
    plt.xlabel('MSE')
    plt.hist(MSE, bins=7, normed=True)
    plt.savefig('mse.png')
    plt.close()

    plt.figure()
    plt.title('Adjusted return in {k}-fold cross validation'.format(**locals()))
    plt.ylabel('Frequency')
    plt.xlabel('Adjusted Return (%)')
    plt.hist(r, bins=10, normed=True)
    plt.savefig('adj_return.png')
    plt.close()

    plt.figure()
    plt.title('Sharpe ratio in {k}-fold cross validation'.format(**locals()))
    plt.ylabel('Frequency')
    plt.xlabel('Sharpe')
    plt.hist(s, bins=10, normed=True)
    plt.savefig('sharpe.png')
    plt.close()

if __name__ == '__main__':
    debug()
    # parameter_tuning(6, 0.2)
    # convergence(minN=50, maxN=1500, spaceN=10, mult=3e1, testN=2000)
    # cross_val()
