#!/usr/bin/python

dc = 0

import sys
import string
import datetime

class Switch:
    def __init__(self, value): 
        self._val = value

    def __enter__(self): 
        return self

    def __exit__(self, type, value, traceback): 
        return False # Allows traceback to occur

    def __call__(self, cond, *mconds): 
        return self._val in (cond,)+mconds

from datetime import date

hoje = date.today()

dt_processamento = hoje.strftime('%d/%m/%Y')

def D1(day):
    data = date.fromordinal(day.toordinal()-1)
    return data

def D2(day):
    data = date.fromordinal(day.toordinal()-2)
    return data

def D3(day):
    data = date.fromordinal(day.toordinal()-3)
    return data


if (date.isoweekday(hoje) == 1):
    print("teste ok")
    if (D3(hoje).month == D1(hoje).month):
        data_mvt = D3(hoje)
        dc += 1
    if (D2(hoje).month == D1(hoje).month):
        dc += 2
else:
    data_mvt = D1(hoje);
    dc += 1

mm_ref = data_mvt.month
aa_ref = data_mvt.year
dt_mvt = data_mvt.strftime('%d/%m/%Y')


with Switch (mm_ref) as case:
    if case (1):
        dc_mes = 31
    if case (2):
        dc_mes = 28
    if case (3):
        dc_mes = 31
    if case (4):
        dc_mes = 30
    if case (5):
        dc_mes = 31
    if case (6):
        dc_mes = 30
    if case (7):
        dc_mes = 31
    if case (8):
        dc_mes = 31
    if case (9):
        dc_mes = 30
    if case (10):
        dc_mes = 31
    if case (11):
        dc_mes = 30
    if case (12):
        dc_mes = 31

#@outputSchema("MSD:tuple(msd:bigdecimal, dt_processamento:chararray, aa_ref:int, mm_ref:int, dt_mvt:chararray, dc:int, dc_mes:int)")
def msd(input):
  msd = input / dc_mes * dc * -1
  return (msd, dt_processamento, aa_ref, mm_ref, dt_mvt, dc, dc_mes)
