import numpy as np
from sklearn.linear_model import LinearRegression
from features import retrieve_feature_data, retrieve_features_for_company
from stock.stocks import get_stock_quote, get_avg_stock_quote

def s(x):
    '''Maps [0, 1] to [0, in)'''
    return np.tan(x * np.pi / 2)

def to_matrix(data):
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
        Y[i][0] = datum['stock_improvement']
    return np.clip(X, -1, 10), Y

def fit(total):
    data = retrieve_feature_data(total, rand=False)
    X, Y = to_matrix(data)
    reg = LinearRegression()
    reg.fit(X, Y)
    return reg

def demo(year,reg):
    print("type 'quit' to stop")
    Q = False
    while not Q:
        cmd = input("Company>")
        if cmd == 'quit': 
            Q = True
            print("Bye!")
        else:
            cdata = retrieve_features_for_company(cmd,year)
            if not cdata is None:
                X, Y = to_matrix([cdata])
                pred = reg.predict(X).tolist()[0][0]
                avg = get_avg_stock_quote(cdata['ticker'],'2016-01-01','2016-06-30')
                act = avg * cdata['stock_improvement']
                pre = avg * pred
                yst = get_stock_quote(cdata['ticker'],'2016-12-8')
                print(cdata['name'])
                print("Predicted:          {:10.2f}        ({:5.2f})".format(pre,pred))
                print("Actual:             {:10.2f}        ({:5.2f})".format(act,cdata['stock_improvement']))
                if yst != 0: print("Current:            {:10.2f}".format(yst))
            else:
                print("I'm sorry Dave, I afraid I can't do that")

if __name__ == '__main__':
    demo(2016,fit(9999))
