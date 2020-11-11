# 同步股票相关的数据
# coding:utf-8

import jqdatasdk as sdk
from jqdatasdk import finance
from jqdatasdk import query
from jqdatasdk import macro

from app import join_quant as jq

from data import mysql_util as my
import time
import math
import pandas as pd
from datetime import datetime


def is_nan(x):
    if math.isnan(x):
        return 0
    else:
        return x


def is_none(x):
    if x is None:
        return 0
    else:
        return x


# 获取龙虎榜数据
def get_billboard(start_date=None, end_date=None):
    jq.login()

    billboard_data = sdk.get_billboard_list(stock_list=None, start_date=start_date, end_date=end_date, count=1)

    billboard_list = []
    for index in billboard_data.index:
        index_billboard_data = billboard_data.iloc[index]

        code = index_billboard_data['code']
        date = index_billboard_data['day'].strftime('%Y-%m-%d')
        direction = index_billboard_data['direction']
        abnormal_code = int(index_billboard_data['abnormal_code'])
        abnormal_name = index_billboard_data['abnormal_name']
        sales_depart_name = index_billboard_data['sales_depart_name']
        rank = int(index_billboard_data['rank'])
        buy_value = is_nan(float(index_billboard_data['buy_value']))
        buy_rate = is_nan(float(index_billboard_data['buy_rate']))
        sell_value = is_nan(float(index_billboard_data['sell_value']))
        sell_rate = is_nan(float(index_billboard_data['sell_rate']))
        net_value = is_nan(float(index_billboard_data['net_value']))
        amount = is_nan(float(index_billboard_data['amount']))

        index_billboard = (
            code, date, direction, abnormal_code, abnormal_name, sales_depart_name, rank, buy_value, buy_rate,
            sell_value, sell_rate, net_value, amount)
        print(index_billboard)
        billboard_list.append(index_billboard)

    insert_sql = "insert into billboard(code, date, direction, abnormal_code, abnormal_name, sales_depart_name, `rank`, buy_value, buy_rate, sell_value, sell_rate, net_value, amount) " \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, billboard_list)


# 财务指标数据(一季度更新)
def get_fundamentals(date):
    stocks_sql = "select code from security"
    stock_codes = my.select_all(stocks_sql, ())

    jq.login()
    fundamental_list = []

    for stock_code in stock_codes:
        code = stock_code['code']

        fundamental_data = sdk.get_fundamentals(sdk.query(sdk.indicator).filter(sdk.valuation.code == code), date)
        if fundamental_data.empty:
            continue

        index_fundamental = fundamental_data.iloc[0]

        pubDate = index_fundamental['pubDate']
        eps = is_nan(float(index_fundamental['eps']))
        adjusted_profit = is_nan(float(index_fundamental['adjusted_profit']))
        operating_profit = is_nan(float(index_fundamental['operating_profit']))
        value_change_profit = is_nan(float(index_fundamental['value_change_profit']))
        roe = is_nan(float(index_fundamental['roe']))
        inc_return = is_nan(float(index_fundamental['inc_return']))
        roa = is_nan(float(index_fundamental['roa']))
        net_profit_margin = is_nan(float(index_fundamental['net_profit_margin']))
        gross_profit_margin = is_nan(float(index_fundamental['gross_profit_margin']))
        expense_to_total_revenue = is_nan(float(index_fundamental['expense_to_total_revenue']))
        operation_profit_to_total_revenue = is_nan(float(index_fundamental['operation_profit_to_total_revenue']))
        net_profit_to_total_revenue = is_nan(float(index_fundamental['net_profit_to_total_revenue']))
        operating_expense_to_total_revenue = is_nan(float(index_fundamental['operating_expense_to_total_revenue']))
        ga_expense_to_total_revenue = is_nan(float(index_fundamental['ga_expense_to_total_revenue']))
        financing_expense_to_total_revenue = is_nan(float(index_fundamental['financing_expense_to_total_revenue']))
        operating_profit_to_profit = is_nan(float(index_fundamental['operating_profit_to_profit']))
        invesment_profit_to_profit = is_nan(float(index_fundamental['invesment_profit_to_profit']))
        adjusted_profit_to_profit = is_nan(float(index_fundamental['adjusted_profit_to_profit']))
        goods_sale_and_service_to_revenue = is_nan(float(index_fundamental['goods_sale_and_service_to_revenue']))
        ocf_to_revenue = is_nan(float(index_fundamental['ocf_to_revenue']))
        ocf_to_operating_profit = is_nan(float(index_fundamental['ocf_to_operating_profit']))
        inc_total_revenue_year_on_year = is_nan(float(index_fundamental['inc_total_revenue_year_on_year']))
        inc_total_revenue_annual = is_nan(float(index_fundamental['inc_total_revenue_annual']))
        inc_revenue_year_on_year = is_nan(float(index_fundamental['inc_revenue_year_on_year']))
        inc_revenue_annual = is_nan(float(index_fundamental['inc_revenue_annual']))
        inc_operation_profit_year_on_year = is_nan(float(index_fundamental['inc_operation_profit_year_on_year']))
        inc_operation_profit_annual = is_nan(float(index_fundamental['inc_operation_profit_annual']))
        inc_net_profit_year_on_year = is_nan(float(index_fundamental['inc_net_profit_year_on_year']))
        inc_net_profit_annual = is_nan(float(index_fundamental['inc_net_profit_annual']))
        inc_net_profit_to_shareholders_year_on_year = is_nan(float(
            index_fundamental['inc_net_profit_to_shareholders_year_on_year']))
        inc_net_profit_to_shareholders_annual = is_nan(
            float(index_fundamental['inc_net_profit_to_shareholders_annual']))

        fundamental = (code, pubDate, eps, adjusted_profit, operating_profit, value_change_profit, roe, inc_return, roa,
                       net_profit_margin, gross_profit_margin, expense_to_total_revenue,
                       operation_profit_to_total_revenue, net_profit_to_total_revenue,
                       operating_expense_to_total_revenue, ga_expense_to_total_revenue,
                       financing_expense_to_total_revenue,
                       operating_profit_to_profit, invesment_profit_to_profit, adjusted_profit_to_profit,
                       goods_sale_and_service_to_revenue, ocf_to_revenue, ocf_to_operating_profit,
                       inc_total_revenue_year_on_year,
                       inc_total_revenue_annual, inc_revenue_year_on_year, inc_revenue_annual,
                       inc_operation_profit_year_on_year,
                       inc_operation_profit_annual, inc_net_profit_year_on_year, inc_net_profit_annual,
                       inc_net_profit_to_shareholders_year_on_year,
                       inc_net_profit_to_shareholders_annual)
        fundamental_list.append(fundamental)
        print(fundamental)

    insert_sql = "insert into fundamental(code, pubDate, eps, adjusted_profit, operating_profit, value_change_profit, roe," \
                 " inc_return, roa, net_profit_margin, gross_profit_margin, expense_to_total_revenue, " \
                 " operation_profit_to_total_revenue, net_profit_to_total_revenue, operating_expense_to_total_revenue, " \
                 " ga_expense_to_total_revenue,financing_expense_to_total_revenue,operating_profit_to_profit, invesment_profit_to_profit, " \
                 " adjusted_profit_to_profit,goods_sale_and_service_to_revenue, ocf_to_revenue, ocf_to_operating_profit," \
                 " inc_total_revenue_year_on_year,inc_total_revenue_annual, inc_revenue_year_on_year, inc_revenue_annual," \
                 " inc_operation_profit_year_on_year,inc_operation_profit_annual, inc_net_profit_year_on_year, inc_net_profit_annual," \
                 " inc_net_profit_to_shareholders_year_on_year,inc_net_profit_to_shareholders_annual) " \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, fundamental_list)


# 合并现金流量表(一季度更新)
def get_cash_flow():
    stocks_sql = "select code from security"
    stock_codes = my.select_all(stocks_sql, ())

    jq.login()
    cash_flow_list = []

    for stock_code in stock_codes:
        code = stock_code['code']

        cash_flow_data = finance.run_query(
            sdk.query(finance.STK_CASHFLOW_STATEMENT).filter(finance.STK_CASHFLOW_STATEMENT.code == code).filter(
                finance.STK_CASHFLOW_STATEMENT.report_type == 0).order_by(
                finance.STK_CASHFLOW_STATEMENT.pub_date.desc()).limit(1))

        if cash_flow_data.empty:
            continue

        index_cash_flow = cash_flow_data.iloc[0]
        pub_date = index_cash_flow['pub_date'].strftime('%Y-%m-%d')

        exist_sql = "select count(1) count from cash_flow where code = %s and pub_date = %s"
        exist = my.select_one(exist_sql, (code, pub_date))
        if exist['count'] > 0:
            print('%s had init', code)
            continue

        company_name = index_cash_flow['company_name']
        start_date = index_cash_flow['start_date'].strftime('%Y-%m-%d')
        end_date = index_cash_flow['end_date'].strftime('%Y-%m-%d')
        goods_sale_and_service_render_cash = float(is_none(index_cash_flow['goods_sale_and_service_render_cash']))
        tax_levy_refund = float(is_none(index_cash_flow['tax_levy_refund']))
        subtotal_operate_cash_inflow = float(is_none(index_cash_flow['subtotal_operate_cash_inflow']))
        goods_and_services_cash_paid = float(is_none(index_cash_flow['goods_and_services_cash_paid']))
        staff_behalf_paid = float(is_none(index_cash_flow['staff_behalf_paid']))
        tax_payments = float(is_none(index_cash_flow['tax_payments']))
        subtotal_operate_cash_outflow = float(is_none(index_cash_flow['subtotal_operate_cash_outflow']))
        net_operate_cash_flow = float(is_none(index_cash_flow['net_operate_cash_flow']))
        invest_withdrawal_cash = float(is_none(index_cash_flow['invest_withdrawal_cash']))
        invest_proceeds = float(is_none(index_cash_flow['invest_proceeds']))
        fix_intan_other_asset_dispo_cash = float(is_none(index_cash_flow['fix_intan_other_asset_dispo_cash']))
        net_cash_deal_subcompany = float(is_none(index_cash_flow['net_cash_deal_subcompany']))
        subtotal_invest_cash_inflow = float(is_none(index_cash_flow['subtotal_invest_cash_inflow']))
        fix_intan_other_asset_acqui_cash = float(is_none(index_cash_flow['fix_intan_other_asset_acqui_cash']))
        invest_cash_paid = float(is_none(index_cash_flow['invest_cash_paid']))
        impawned_loan_net_increase = float(is_none(index_cash_flow['impawned_loan_net_increase']))
        net_cash_from_sub_company = float(is_none(index_cash_flow['net_cash_from_sub_company']))
        subtotal_invest_cash_outflow = float(is_none(index_cash_flow['subtotal_invest_cash_outflow']))
        net_invest_cash_flow = float(is_none(index_cash_flow['net_invest_cash_flow']))
        cash_from_invest = float(is_none(index_cash_flow['cash_from_invest']))
        cash_from_borrowing = float(is_none(index_cash_flow['cash_from_borrowing']))
        cash_from_bonds_issue = float(is_none(index_cash_flow['cash_from_bonds_issue']))
        subtotal_finance_cash_inflow = float(is_none(index_cash_flow['subtotal_finance_cash_inflow']))
        borrowing_repayment = float(is_none(index_cash_flow['borrowing_repayment']))
        dividend_interest_payment = float(is_none(index_cash_flow['dividend_interest_payment']))
        subtotal_finance_cash_outflow = float(is_none(index_cash_flow['subtotal_finance_cash_outflow']))
        net_finance_cash_flow = float(is_none(index_cash_flow['net_finance_cash_flow']))
        exchange_rate_change_effect = float(is_none(index_cash_flow['exchange_rate_change_effect']))
        other_reason_effect_cash = float(is_none(index_cash_flow['other_reason_effect_cash']))
        cash_equivalent_increase = float(is_none(index_cash_flow['cash_equivalent_increase']))
        cash_equivalents_at_beginning = float(is_none(index_cash_flow['cash_equivalents_at_beginning']))
        cash_and_equivalents_at_end = float(is_none(index_cash_flow['cash_and_equivalents_at_end']))
        net_profit = float(is_none(index_cash_flow['net_profit']))
        assets_depreciation_reserves = float(is_none(index_cash_flow['assets_depreciation_reserves']))
        fixed_assets_depreciation = float(is_none(index_cash_flow['fixed_assets_depreciation']))
        intangible_assets_amortization = float(is_none(index_cash_flow['intangible_assets_amortization']))
        defferred_expense_amortization = float(is_none(index_cash_flow['defferred_expense_amortization']))
        fix_intan_other_asset_dispo_loss = float(is_none(index_cash_flow['fix_intan_other_asset_dispo_loss']))
        fixed_asset_scrap_loss = float(is_none(index_cash_flow['fixed_asset_scrap_loss']))
        fair_value_change_loss = float(is_none(index_cash_flow['fair_value_change_loss']))
        financial_cost = float(is_none(index_cash_flow['financial_cost']))
        invest_loss = float(is_none(index_cash_flow['invest_loss']))
        deffered_tax_asset_decrease = float(is_none(index_cash_flow['deffered_tax_asset_decrease']))
        deffered_tax_liability_increase = float(is_none(index_cash_flow['deffered_tax_liability_increase']))
        inventory_decrease = float(is_none(index_cash_flow['inventory_decrease']))
        operate_receivables_decrease = float(is_none(index_cash_flow['operate_receivables_decrease']))
        operate_payable_increase = float(is_none(index_cash_flow['operate_payable_increase']))
        others = float(is_none(index_cash_flow['others']))
        net_operate_cash_flow_indirect = float(is_none(index_cash_flow['net_operate_cash_flow_indirect']))
        debt_to_capital = float(is_none(index_cash_flow['debt_to_capital']))
        cbs_expiring_in_one_year = float(is_none(index_cash_flow['cbs_expiring_in_one_year']))
        financial_lease_fixed_assets = float(is_none(index_cash_flow['financial_lease_fixed_assets']))
        cash_at_end = float(is_none(index_cash_flow['cash_at_end']))
        cash_at_beginning = float(is_none(index_cash_flow['cash_at_beginning']))
        equivalents_at_end = float(is_none(index_cash_flow['equivalents_at_end']))
        equivalents_at_beginning = float(is_none(index_cash_flow['equivalents_at_beginning']))
        other_reason_effect_cash_indirect = float(is_none(index_cash_flow['other_reason_effect_cash_indirect']))
        cash_equivalent_increase_indirect = float(is_none(index_cash_flow['cash_equivalent_increase_indirect']))
        net_deposit_increase = float(is_none(index_cash_flow['net_deposit_increase']))
        net_borrowing_from_central_bank = float(is_none(index_cash_flow['net_borrowing_from_central_bank']))
        net_borrowing_from_finance_co = float(is_none(index_cash_flow['net_borrowing_from_finance_co']))
        net_original_insurance_cash = float(is_none(index_cash_flow['net_original_insurance_cash']))
        net_cash_received_from_reinsurance_business = float(
            is_none(index_cash_flow['net_cash_received_from_reinsurance_business']))
        net_insurer_deposit_investment = float(is_none(index_cash_flow['net_insurer_deposit_investment']))
        net_deal_trading_assets = float(is_none(index_cash_flow['net_deal_trading_assets']))
        interest_and_commission_cashin = float(is_none(index_cash_flow['interest_and_commission_cashin']))
        net_increase_in_placements = float(is_none(index_cash_flow['net_increase_in_placements']))
        net_buyback = float(is_none(index_cash_flow['net_buyback']))
        net_loan_and_advance_increase = float(is_none(index_cash_flow['net_loan_and_advance_increase']))
        net_deposit_in_cb_and_ib = float(is_none(index_cash_flow['net_deposit_in_cb_and_ib']))
        original_compensation_paid = float(is_none(index_cash_flow['original_compensation_paid']))
        handling_charges_and_commission = float(is_none(index_cash_flow['handling_charges_and_commission']))
        policy_dividend_cash_paid = float(is_none(index_cash_flow['policy_dividend_cash_paid']))
        cash_from_mino_s_invest_sub = float(is_none(index_cash_flow['cash_from_mino_s_invest_sub']))
        proceeds_from_sub_to_mino_s = float(is_none(index_cash_flow['proceeds_from_sub_to_mino_s']))
        investment_property_depreciation = float(is_none(index_cash_flow['investment_property_depreciation']))

        # pd.set_option('display.max_columns', None)
        # pd.set_option('display.max_rows', None)
        # pd.set_option('max_colwidth', 100)

        cash_flow = (code, company_name, pub_date, start_date, end_date,
                     goods_sale_and_service_render_cash, tax_levy_refund,
                     subtotal_operate_cash_inflow, goods_and_services_cash_paid,
                     staff_behalf_paid, tax_payments, subtotal_operate_cash_outflow,
                     net_operate_cash_flow, invest_withdrawal_cash, invest_proceeds,
                     fix_intan_other_asset_dispo_cash, net_cash_deal_subcompany,
                     subtotal_invest_cash_inflow, fix_intan_other_asset_acqui_cash,
                     invest_cash_paid, impawned_loan_net_increase,
                     net_cash_from_sub_company, subtotal_invest_cash_outflow,
                     net_invest_cash_flow, cash_from_invest, cash_from_borrowing,
                     cash_from_bonds_issue, subtotal_finance_cash_inflow,
                     borrowing_repayment, dividend_interest_payment,
                     subtotal_finance_cash_outflow, net_finance_cash_flow,
                     exchange_rate_change_effect, other_reason_effect_cash,
                     cash_equivalent_increase, cash_equivalents_at_beginning,
                     cash_and_equivalents_at_end, net_profit,
                     assets_depreciation_reserves, fixed_assets_depreciation,
                     intangible_assets_amortization, defferred_expense_amortization,
                     fix_intan_other_asset_dispo_loss, fixed_asset_scrap_loss,
                     fair_value_change_loss, financial_cost, invest_loss,
                     deffered_tax_asset_decrease, deffered_tax_liability_increase,
                     inventory_decrease, operate_receivables_decrease,
                     operate_payable_increase, others, net_operate_cash_flow_indirect,
                     debt_to_capital, cbs_expiring_in_one_year,
                     financial_lease_fixed_assets, cash_at_end, cash_at_beginning,
                     equivalents_at_end, equivalents_at_beginning,
                     other_reason_effect_cash_indirect,
                     cash_equivalent_increase_indirect, net_deposit_increase,
                     net_borrowing_from_central_bank, net_borrowing_from_finance_co,
                     net_original_insurance_cash,
                     net_cash_received_from_reinsurance_business,
                     net_insurer_deposit_investment, net_deal_trading_assets,
                     interest_and_commission_cashin, net_increase_in_placements,
                     net_buyback, net_loan_and_advance_increase,
                     net_deposit_in_cb_and_ib, original_compensation_paid,
                     handling_charges_and_commission, policy_dividend_cash_paid,
                     cash_from_mino_s_invest_sub, proceeds_from_sub_to_mino_s,
                     investment_property_depreciation)
        print(cash_flow)
        cash_flow_list.append(cash_flow)

    insert_sql = "insert into cash_flow(code, company_name, pub_date, start_date, end_date," \
                 " goods_sale_and_service_render_cash, tax_levy_refund," \
                 " subtotal_operate_cash_inflow, goods_and_services_cash_paid," \
                 " staff_behalf_paid, tax_payments, subtotal_operate_cash_outflow," \
                 " net_operate_cash_flow, invest_withdrawal_cash, invest_proceeds," \
                 " fix_intan_other_asset_dispo_cash, net_cash_deal_subcompany," \
                 " subtotal_invest_cash_inflow, fix_intan_other_asset_acqui_cash," \
                 " invest_cash_paid, impawned_loan_net_increase," \
                 " net_cash_from_sub_company, subtotal_invest_cash_outflow," \
                 " net_invest_cash_flow, cash_from_invest, cash_from_borrowing," \
                 " cash_from_bonds_issue, subtotal_finance_cash_inflow," \
                 " borrowing_repayment, dividend_interest_payment," \
                 " subtotal_finance_cash_outflow, net_finance_cash_flow," \
                 " exchange_rate_change_effect, other_reason_effect_cash," \
                 " cash_equivalent_increase, cash_equivalents_at_beginning," \
                 " cash_and_equivalents_at_end, net_profit," \
                 " assets_depreciation_reserves, fixed_assets_depreciation," \
                 " intangible_assets_amortization, defferred_expense_amortization," \
                 " fix_intan_other_asset_dispo_loss, fixed_asset_scrap_loss," \
                 " fair_value_change_loss, financial_cost, invest_loss," \
                 " deffered_tax_asset_decrease, deffered_tax_liability_increase," \
                 " inventory_decrease, operate_receivables_decrease," \
                 " operate_payable_increase, others, net_operate_cash_flow_indirect," \
                 " debt_to_capital, cbs_expiring_in_one_year," \
                 " financial_lease_fixed_assets, cash_at_end, cash_at_beginning," \
                 " equivalents_at_end, equivalents_at_beginning," \
                 " other_reason_effect_cash_indirect," \
                 " cash_equivalent_increase_indirect, net_deposit_increase," \
                 " net_borrowing_from_central_bank, net_borrowing_from_finance_co," \
                 " net_original_insurance_cash," \
                 " net_cash_received_from_reinsurance_business," \
                 " net_insurer_deposit_investment, net_deal_trading_assets," \
                 " interest_and_commission_cashin, net_increase_in_placements," \
                 " net_buyback, net_loan_and_advance_increase," \
                 " net_deposit_in_cb_and_ib, original_compensation_paid," \
                 " handling_charges_and_commission, policy_dividend_cash_paid," \
                 " cash_from_mino_s_invest_sub, proceeds_from_sub_to_mino_s," \
                 " investment_property_depreciation) " \
                 "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, cash_flow_list)


# get_cash_flow()
