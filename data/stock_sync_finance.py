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


# 获取合并资产负债表
def get_balance_sheet():
    stocks_sql = "select code from security"
    stock_codes = my.select_all(stocks_sql, ())

    jq.login()
    balance_sheet_list = []

    for stock_code in stock_codes:
        code = stock_code['code']

        balance_sheet_data = finance.run_query(
            sdk.query(finance.STK_BALANCE_SHEET).filter(finance.STK_BALANCE_SHEET.code == code).filter(
                finance.STK_BALANCE_SHEET.report_type == 0).order_by(
                finance.STK_BALANCE_SHEET.pub_date.desc()).limit(1))

        if balance_sheet_data.empty:
            continue

        index_balance_sheet = balance_sheet_data.iloc[0]
        pub_date = index_balance_sheet['pub_date'].strftime('%Y-%m-%d')

        # exist_sql = "select count(1) count from balance_sheet where code = %s and pub_date = %s"
        # exist = my.select_one(exist_sql, (code, pub_date))
        # if exist['count'] > 0:
        #     print('%s had init', code)
        #     continue

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 120)

        company_name = index_balance_sheet['company_name']
        start_date = index_balance_sheet['start_date'].strftime('%Y-%m-%d')
        end_date = index_balance_sheet['end_date'].strftime('%Y-%m-%d')
        cash_equivalents = float(is_none(index_balance_sheet['cash_equivalents']))
        trading_assets = float(is_none(index_balance_sheet['trading_assets']))
        bill_receivable = float(is_none(index_balance_sheet['bill_receivable']))
        account_receivable = float(is_none(index_balance_sheet['account_receivable']))
        advance_payment = float(is_none(index_balance_sheet['advance_payment']))
        other_receivable = float(is_none(index_balance_sheet['other_receivable']))
        affiliated_company_receivable = float(is_none(index_balance_sheet['affiliated_company_receivable']))
        interest_receivable = float(is_none(index_balance_sheet['interest_receivable']))
        dividend_receivable = float(is_none(index_balance_sheet['dividend_receivable']))
        inventories = float(is_none(index_balance_sheet['inventories']))
        expendable_biological_asset = float(is_none(index_balance_sheet['expendable_biological_asset']))
        non_current_asset_in_one_year = float(is_none(index_balance_sheet['non_current_asset_in_one_year']))
        total_current_assets = float(is_none(index_balance_sheet['total_current_assets']))
        hold_for_sale_assets = float(is_none(index_balance_sheet['hold_for_sale_assets']))
        hold_to_maturity_investments = float(is_none(index_balance_sheet['hold_to_maturity_investments']))
        longterm_receivable_account = float(is_none(index_balance_sheet['longterm_receivable_account']))
        longterm_equity_invest = float(is_none(index_balance_sheet['longterm_equity_invest']))
        investment_property = float(is_none(index_balance_sheet['investment_property']))
        fixed_assets = float(is_none(index_balance_sheet['fixed_assets']))
        constru_in_process = float(is_none(index_balance_sheet['constru_in_process']))
        construction_materials = float(is_none(index_balance_sheet['construction_materials']))
        fixed_assets_liquidation = float(is_none(index_balance_sheet['fixed_assets_liquidation']))
        biological_assets = float(is_none(index_balance_sheet['biological_assets']))
        oil_gas_assets = float(is_none(index_balance_sheet['oil_gas_assets']))
        intangible_assets = float(is_none(index_balance_sheet['intangible_assets']))
        development_expenditure = float(is_none(index_balance_sheet['development_expenditure']))
        good_will = float(is_none(index_balance_sheet['good_will']))
        long_deferred_expense = float(is_none(index_balance_sheet['long_deferred_expense']))
        deferred_tax_assets = float(is_none(index_balance_sheet['deferred_tax_assets']))
        total_non_current_assets = float(is_none(index_balance_sheet['total_non_current_assets']))
        total_assets = float(is_none(index_balance_sheet['total_assets']))
        shortterm_loan = float(is_none(index_balance_sheet['shortterm_loan']))
        trading_liability = float(is_none(index_balance_sheet['trading_liability']))
        notes_payable = float(is_none(index_balance_sheet['notes_payable']))
        accounts_payable = float(is_none(index_balance_sheet['accounts_payable']))
        advance_peceipts = float(is_none(index_balance_sheet['advance_peceipts']))
        salaries_payable = float(is_none(index_balance_sheet['salaries_payable']))
        taxs_payable = float(is_none(index_balance_sheet['taxs_payable']))
        interest_payable = float(is_none(index_balance_sheet['interest_payable']))
        dividend_payable = float(is_none(index_balance_sheet['dividend_payable']))
        other_payable = float(is_none(index_balance_sheet['other_payable']))
        affiliated_company_payable = float(is_none(index_balance_sheet['affiliated_company_payable']))
        non_current_liability_in_one_year = float(is_none(index_balance_sheet['non_current_liability_in_one_year']))
        total_current_liability = float(is_none(index_balance_sheet['total_current_liability']))
        longterm_loan = float(is_none(index_balance_sheet['longterm_loan']))
        bonds_payable = float(is_none(index_balance_sheet['bonds_payable']))
        longterm_account_payable = float(is_none(index_balance_sheet['longterm_account_payable']))
        specific_account_payable = float(is_none(index_balance_sheet['specific_account_payable']))
        estimate_liability = float(is_none(index_balance_sheet['estimate_liability']))
        deferred_tax_liability = float(is_none(index_balance_sheet['deferred_tax_liability']))
        total_non_current_liability = float(is_none(index_balance_sheet['total_non_current_liability']))
        total_liability = float(is_none(index_balance_sheet['total_liability']))
        paidin_capital = float(is_none(index_balance_sheet['paidin_capital']))
        capital_reserve_fund = float(is_none(index_balance_sheet['capital_reserve_fund']))
        specific_reserves = float(is_none(index_balance_sheet['specific_reserves']))
        surplus_reserve_fund = float(is_none(index_balance_sheet['surplus_reserve_fund']))
        treasury_stock = float(is_none(index_balance_sheet['treasury_stock']))
        retained_profit = float(is_none(index_balance_sheet['retained_profit']))
        equities_parent_company_owners = float(is_none(index_balance_sheet['equities_parent_company_owners']))
        minority_interests = float(is_none(index_balance_sheet['minority_interests']))
        foreign_currency_report_conv_diff = float(is_none(index_balance_sheet['foreign_currency_report_conv_diff']))
        irregular_item_adjustment = float(is_none(index_balance_sheet['irregular_item_adjustment']))
        total_owner_equities = float(is_none(index_balance_sheet['total_owner_equities']))
        total_sheet_owner_equities = float(is_none(index_balance_sheet['total_sheet_owner_equities']))
        other_comprehensive_income = float(is_none(index_balance_sheet['other_comprehensive_income']))
        deferred_earning = float(is_none(index_balance_sheet['deferred_earning']))
        settlement_provi = float(is_none(index_balance_sheet['settlement_provi']))
        lend_capital = float(is_none(index_balance_sheet['lend_capital']))
        loan_and_advance_current_assets = float(is_none(index_balance_sheet['loan_and_advance_current_assets']))
        derivative_financial_asset = float(is_none(index_balance_sheet['derivative_financial_asset']))
        insurance_receivables = float(is_none(index_balance_sheet['insurance_receivables']))
        reinsurance_receivables = float(is_none(index_balance_sheet['reinsurance_receivables']))
        reinsurance_contract_reserves_receivable = float(
            is_none(index_balance_sheet['reinsurance_contract_reserves_receivable']))
        bought_sellback_assets = float(is_none(index_balance_sheet['bought_sellback_assets']))
        hold_sale_asset = float(is_none(index_balance_sheet['hold_sale_asset']))
        loan_and_advance_noncurrent_assets = float(is_none(index_balance_sheet['loan_and_advance_noncurrent_assets']))
        borrowing_from_centralbank = float(is_none(index_balance_sheet['borrowing_from_centralbank']))
        deposit_in_interbank = float(is_none(index_balance_sheet['deposit_in_interbank']))
        borrowing_capital = float(is_none(index_balance_sheet['borrowing_capital']))
        derivative_financial_liability = float(is_none(index_balance_sheet['derivative_financial_liability']))
        sold_buyback_secu_proceeds = float(is_none(index_balance_sheet['sold_buyback_secu_proceeds']))
        commission_payable = float(is_none(index_balance_sheet['commission_payable']))
        reinsurance_payables = float(is_none(index_balance_sheet['reinsurance_payables']))
        insurance_contract_reserves = float(is_none(index_balance_sheet['insurance_contract_reserves']))
        proxy_secu_proceeds = float(is_none(index_balance_sheet['proxy_secu_proceeds']))
        receivings_from_vicariously_sold_securities = float(
            is_none(index_balance_sheet['receivings_from_vicariously_sold_securities']))
        hold_sale_liability = float(is_none(index_balance_sheet['hold_sale_liability']))
        estimate_liability_current = float(is_none(index_balance_sheet['estimate_liability_current']))
        deferred_earning_current = float(is_none(index_balance_sheet['deferred_earning_current']))
        preferred_shares_noncurrent = float(is_none(index_balance_sheet['preferred_shares_noncurrent']))
        pepertual_liability_noncurrent = float(is_none(index_balance_sheet['pepertual_liability_noncurrent']))
        longterm_salaries_payable = float(is_none(index_balance_sheet['longterm_salaries_payable']))
        other_equity_tools = float(is_none(index_balance_sheet['other_equity_tools']))
        preferred_shares_equity = float(is_none(index_balance_sheet['preferred_shares_equity']))
        pepertual_liability_equity = float(is_none(index_balance_sheet['pepertual_liability_equity']))

        balance_sheet = (
            code, company_name, pub_date, start_date, end_date, cash_equivalents, trading_assets, bill_receivable,
            account_receivable, advance_payment, other_receivable,
            affiliated_company_receivable, interest_receivable, dividend_receivable, inventories,
            expendable_biological_asset, non_current_asset_in_one_year, total_current_assets, hold_for_sale_assets,
            hold_to_maturity_investments, longterm_receivable_account, longterm_equity_invest, investment_property,
            fixed_assets, constru_in_process, construction_materials,
            fixed_assets_liquidation, biological_assets, oil_gas_assets, intangible_assets, development_expenditure,
            good_will, long_deferred_expense, deferred_tax_assets, total_non_current_assets,
            total_assets, shortterm_loan, trading_liability, notes_payable, accounts_payable, advance_peceipts,
            salaries_payable, taxs_payable, interest_payable, dividend_payable,
            other_payable, affiliated_company_payable, non_current_liability_in_one_year, total_current_liability,
            longterm_loan, bonds_payable, longterm_account_payable, specific_account_payable,
            estimate_liability, deferred_tax_liability, total_non_current_liability, total_liability, paidin_capital,
            capital_reserve_fund, specific_reserves, surplus_reserve_fund, treasury_stock,
            retained_profit, equities_parent_company_owners, minority_interests, foreign_currency_report_conv_diff,
            irregular_item_adjustment, total_owner_equities, total_sheet_owner_equities,
            other_comprehensive_income, deferred_earning, settlement_provi, lend_capital,
            loan_and_advance_current_assets,
            derivative_financial_asset, insurance_receivables, reinsurance_receivables,
            reinsurance_contract_reserves_receivable, bought_sellback_assets, hold_sale_asset,
            loan_and_advance_noncurrent_assets, borrowing_from_centralbank, deposit_in_interbank, borrowing_capital,
            derivative_financial_liability, sold_buyback_secu_proceeds, commission_payable, reinsurance_payables,
            insurance_contract_reserves, proxy_secu_proceeds, receivings_from_vicariously_sold_securities,
            hold_sale_liability, estimate_liability_current, deferred_earning_current, preferred_shares_noncurrent,
            pepertual_liability_noncurrent, longterm_salaries_payable, other_equity_tools,
            preferred_shares_equity, pepertual_liability_equity)
        print(balance_sheet)
        balance_sheet_list.append(balance_sheet)

    insert_sql = "insert into balance_sheet(code, company_name, pub_date, start_date, end_date, cash_equivalents, trading_assets, bill_receivable," \
                 " account_receivable, advance_payment, other_receivable," \
                 " affiliated_company_receivable, interest_receivable, dividend_receivable, inventories," \
                 " expendable_biological_asset, non_current_asset_in_one_year, total_current_assets, hold_for_sale_assets," \
                 " hold_to_maturity_investments, longterm_receivable_account, longterm_equity_invest, investment_property," \
                 " fixed_assets, constru_in_process, construction_materials," \
                 " fixed_assets_liquidation, biological_assets, oil_gas_assets, intangible_assets, development_expenditure," \
                 " good_will, long_deferred_expense, deferred_tax_assets, total_non_current_assets," \
                 " total_assets, shortterm_loan, trading_liability, notes_payable, accounts_payable, advance_peceipts," \
                 " salaries_payable, taxs_payable, interest_payable, dividend_payable," \
                 " other_payable, affiliated_company_payable, non_current_liability_in_one_year, total_current_liability," \
                 " longterm_loan, bonds_payable, longterm_account_payable, specific_account_payable," \
                 " estimate_liability, deferred_tax_liability, total_non_current_liability, total_liability, paidin_capital," \
                 " capital_reserve_fund, specific_reserves, surplus_reserve_fund, treasury_stock," \
                 " retained_profit, equities_parent_company_owners, minority_interests, foreign_currency_report_conv_diff," \
                 " irregular_item_adjustment, total_owner_equities, total_sheet_owner_equities," \
                 " other_comprehensive_income, deferred_earning, settlement_provi, lend_capital," \
                 " loan_and_advance_current_assets," \
                 " derivative_financial_asset, insurance_receivables, reinsurance_receivables," \
                 " reinsurance_contract_reserves_receivable, bought_sellback_assets, hold_sale_asset," \
                 " loan_and_advance_noncurrent_assets, borrowing_from_centralbank, deposit_in_interbank, borrowing_capital," \
                 " derivative_financial_liability, sold_buyback_secu_proceeds, commission_payable, reinsurance_payables," \
                 " insurance_contract_reserves, proxy_secu_proceeds, receivings_from_vicariously_sold_securities," \
                 " hold_sale_liability, estimate_liability_current, deferred_earning_current, preferred_shares_noncurrent," \
                 " pepertual_liability_noncurrent, longterm_salaries_payable, other_equity_tools," \
                 " preferred_shares_equity, pepertual_liability_equity)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                 " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, balance_sheet_list)


# 获取财务相关数据(一季度一次)
def get_finance_data(date):
    # 财务指标
    get_fundamentals(date)

    # 现金流量表
    get_cash_flow()

    # 资产负债表
    get_balance_sheet()

