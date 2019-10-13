#!/usr/bin/python

def oportunidade(dc, dc_mes, cd_fma, ttf, saldo):
  if cd_fma == 0:
    return ((((1 + 0.02462) -1) * saldo) * dc) * -1
  elif cd_fma == 1:
    return (((((1 + ttf / 100) ** 1 / 360)-1) * saldo) * dc) * -1
  elif cd_fma == 2:
    return (((((1 + ttf / 100) ** 1 / dc_mes)-1) * saldo) * dc) * -1
  elif cd_fma == 3:
    return (((ttf / 100) * saldo) * dc) * -1
  elif cd_fma == 4:
    return (((((1 + ttf / 100) ** 1 / 365)-1) * saldo) * dc) * -1
  elif cd_fma == 5:
    return (((((1 + ttf / 100) ** 1 / 252)-1) * saldo) * dc) * -1
  elif cd_fma == 9:
    return (((((1 + ttf / 100) ** 1 / 30)-1) * saldo) * dc) * -1
  elif cd_fma == 10:
    return (((ttf / 100) * saldo) * dc) * -1
  elif cd_fma == 12:
    return (((((1 + ttf / 100) ** 1 / 365)-1) * saldo) * dc) * -1
  elif cd_fma == 13:
    return (((((1 + ttf / 100) ** 1 / 30)-1) * saldo) * dc) * -1


#@outputSchema("eaopt:bigdecimal")
def eaopt(dc, dc_mes, cd_fma, ttf, saldo):
    eaopt = oportunidade(dc, dc_mes, cd_fma, ttf, saldo)
    return eaopt

