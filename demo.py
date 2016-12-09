import numpy as np
from sklearn.linear_model import LinearRegression
from features import retrieve_feature_data, retrieve_features_for_company
from stock.stocks import get_stock_quote, get_avg_stock_quote
from prepare import s, to_matrix, power

def fit(total):
    data = retrieve_feature_data(total, rand=False)
    X, Y = to_matrix(data, power)
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
                X, Y = to_matrix([cdata], power)
                pred = reg.predict(X).tolist()[0][0] ** (1/power)
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
