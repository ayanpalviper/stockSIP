import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import pyxirr
import warnings
import typer

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

remaining_budget = 0.0
current_quantity = 0.0
total_invested = 0.0
month_done_list = []
remaining_div = 0.0
data_store_df = {}
total_dividend = 0.0


def accumulate(open, dividends, date, budget, with_drip, day_of_month_to_buy):
    global remaining_budget
    global current_quantity
    global total_invested
    global month_done_list
    global remaining_div
    global total_dividend
    mb = budget[date.year]
    mb += (remaining_budget + remaining_div)
    bought = False
    if dividends > 0.0 and with_drip:
        remaining_div = current_quantity * dividends
        total_dividend += remaining_div
        # print(str(date) +' '+str(remaining_div))
    is_month_done = (str(date.month) + "/" + str(date.year)) in month_done_list
    if not is_month_done and ((date.day == day_of_month_to_buy) or (date.day > day_of_month_to_buy)):
        if open < mb:
            ans = divmod(mb, open)
            remaining_budget = ans[1]
            total_invested = total_invested + (mb - remaining_budget - remaining_div)
            current_quantity = current_quantity + ans[0]
            month_done_list.append(str(date.month) + "/" + str(date.year))
            bought = True
            current_invested = ans[0] * open
            remaining_div = 0.0
        else:
            remaining_budget = mb
            current_invested = 0.0
        return {"bought": bought, "date": date,
                "remaining_budget": remaining_budget,
                "current_quantity": current_quantity,
                "total_invested": total_invested,
                "current_invested": current_invested,
                "total_dividend": total_dividend}


def assemble_monthly_budgets(start_datetime, starting_monthly_budget, budget_increase_percent):
    monthly_budget_infaltion = {}
    start_year = start_datetime.year
    end_year = datetime.datetime.now().year + 1
    current_budget = starting_monthly_budget
    for yr in range(start_year, end_year):
        monthly_budget_infaltion[yr] = current_budget
        current_budget = current_budget + (current_budget * (budget_increase_percent / 100))
    return monthly_budget_infaltion


def calculate_xirr(results, current_value_of_investment, end_datetime):
    dict_xirr = {end_datetime: current_value_of_investment * -1}
    total_invested = 0.0
    for result in results:
        dt = result['date']
        current_invested = result['current_invested']
        dict_xirr[dt.date()] = current_invested
        total_invested += current_invested
    # print(total_invested)
    return pyxirr.xirr(dict_xirr)


def compute_single():
    stock = input('Please input the NSE stock symbol\n')
    exchange = input('Please input the stock exchange (BSE/NSE)\n')
    if exchange == 'BSE':
        exchange = 'BO'
    elif exchange == 'NSE':
        exchange = 'NS'
    else:
        raise Exception('Incorrect Exchange')
    start_dt = input('Please input the start date (YYYY-MM-DD)\n')
    start_datetime = datetime.datetime.strptime(start_dt, '%Y-%m-%d')
    end_dt = input('Please input the end date (YYYY-MM-DD)\n')
    end_datetime = datetime.datetime.strptime(end_dt, '%Y-%m-%d')
    with_drip = input('Do you want dividends to be reinvested? (Y/N)\n')
    if with_drip == 'Y':
        with_drip = True
    else:
        with_drip = False
    starting_monthly_budget = float(input('Please input starting monthly budget\n'))
    budget_increase_percent = float(input('Please input the rate at which monthly budget will increase every year\n'))
    day_of_month_to_buy = int(input('Which day of the month to buy\n'))

    tkr = yf.Ticker(stock + '.' + exchange)
    his = tkr.history(interval='1d', start=start_dt, end=end_dt, back_adjust=True)

    his.reset_index(inplace=True)

    budget = assemble_monthly_budgets(start_datetime, starting_monthly_budget, budget_increase_percent)

    vec_accumulate = np.vectorize(accumulate, otypes=[dict])
    result = vec_accumulate(his['Open'], his['Dividends'], his['Date'], budget, with_drip, day_of_month_to_buy)
    result = result[result != np.array(None)]
    # print(result)
    total_accumulated_quantity = result[-1]['current_quantity']
    total_invested_amount = result[-1]['total_invested']
    current_value_of_investment = total_accumulated_quantity * his.iloc[-1]['Close']

    calc_xirr = calculate_xirr(result, current_value_of_investment, end_datetime)

    print('XIRR ' + str(calc_xirr * 100))

    print('Total Accumulated Quantity ' + str(total_accumulated_quantity))
    print('Total Amount Invested ' + str(total_invested_amount))
    print('Average Price Per Share ' + str(total_invested_amount / total_accumulated_quantity))

    print('Current Value Of Investment ' + str(current_value_of_investment))


def process(index, symbol, exchange, start_dt, end_dt, with_drip, start_budget, roi, day_of_month_to_buy):
    global remaining_budget
    global current_quantity
    global total_invested
    global month_done_list
    global remaining_div
    global data_store_df
    global total_dividend

    if exchange == 'BSE':
        exchange = 'BO'
    elif exchange == 'NSE':
        exchange = 'NS'
    else:
        raise Exception('Incorrect Exchange')

    start_datetime = start_dt.to_pydatetime()
    end_datetime = end_dt.to_pydatetime()

    ticker = symbol + '.' + exchange

    if ticker in data_store_df.keys():
        c_his = data_store_df[ticker]
        # print('using existing ticker '+ticker)
    else:
        tkr = yf.Ticker(ticker)
        c_his = tkr.history(interval='1d', period='max', back_adjust=True)
        c_his.reset_index(inplace=True)
        data_store_df[ticker] = c_his
        # print('generated new ticker '+ticker)

    # print(c_his.head())
    # print(c_his.tail())

    his = c_his[(c_his['Date'].dt.date >= start_datetime.date()) & (c_his['Date'].dt.date <= end_datetime.date())]

    budget = assemble_monthly_budgets(start_datetime, start_budget, roi)

    vec_accumulate = np.vectorize(accumulate, otypes=[dict])
    result = vec_accumulate(his['Open'], his['Dividends'], his['Date'], budget, with_drip, day_of_month_to_buy)
    result = result[result != np.array(None)]
    # print(result)
    total_accumulated_quantity = result[-1]['current_quantity']
    total_invested_amount = result[-1]['total_invested']
    current_value_of_investment = total_accumulated_quantity * his.iloc[-1]['Close']
    temp_total_div = result[-1]['total_dividend']
    calc_xirr = calculate_xirr(result, current_value_of_investment, end_datetime)

    # print('DONE WITH '+symbol)

    remaining_budget = 0.0
    current_quantity = 0.0
    total_invested = 0.0
    month_done_list = []
    remaining_div = 0.0
    total_dividend = 0.0

    return {"index": index,
            "xirr": calc_xirr * 100,
            "quantity": total_accumulated_quantity,
            "amount_invested": total_invested_amount,
            "price_per_share": total_invested_amount / total_accumulated_quantity,
            "total_value": current_value_of_investment,
            "total_dividend": temp_total_div}


def compute_multiple(input_file_path, output_file_path):
    df = pd.read_excel(input_file_path, sheet_name='data')
    vec_process = np.vectorize(process, otypes=[dict])
    results = vec_process(df.index, df['Symbol'],
                          df['Exchange'], df['Start Date'], df['End Date'],
                          df['Reinvest Dividends'], df['Starting Budget'],
                          df['Rate Of Increase'], df['Day Of Purchase']
                          )

    for result in results:
        df.at[result['index'], ' XIRR'] = result['xirr']
        df.at[result['index'], 'Total Accumulated Quantity'] = result['quantity']
        df.at[result['index'], 'Total Amount Invested'] = result['amount_invested']
        df.at[result['index'], 'Price Per Share'] = result['price_per_share']
        df.at[result['index'], 'Total Value'] = result['total_value']
        df.at[result['index'], 'Total Dividends'] = result['total_dividend']

    # print(df)
    df.to_excel(output_file_path, sheet_name='data', index=False)


def main(mode: int = 1, input_file_path: str = "input.xlsx", output_file_path: str = "output.xlsx"):
    if mode == 1:
        compute_multiple(input_file_path, output_file_path)
    elif mode == 2:
        compute_single()
    else:
        raise Exception('Invalid Mode')


if __name__ == '__main__':
    typer.run(main)
