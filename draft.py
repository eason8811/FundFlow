from matplotlib.pylab import date2num


# wdyx.info()
# print(wdyx.head())

def date_to_num(dates):
    num_time = []
    for date in dates:
        num_date = date2num(date)
        num_time.append(num_date)
    return num_time


s = [5, 2, 9, 3, 6]
d = {'7': 5, '9': 2, '6': 9}
s = s.sort()

print('finish')
