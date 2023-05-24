import sys

def main(sales_gas_alloc_e3m3: float, gas_gross_e3m3: float, work_interest: float, gas_shrink: float) -> float:

    if gas_shrink != None:
        return gas_shrink * work_interest * 0.01
    
    if sales_gas_alloc_e3m3 != None:
        return sales_gas_alloc_e3m3 * work_interest * 0.01
        
    return 0 if gas_gross_e3m3 == None else gas_gross_e3m3 * work_interest * 0.01


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore
