import sys

def main(alloc_m3: float, gross_m3: float, work_interest: float) -> float:

    if alloc_m3 == None:
        return 0 if gross_m3 == None else gross_m3 * work_interest * 0.01
        
    return 0 if alloc_m3 == None else alloc_m3 * work_interest * 0.01
    
    



# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore
