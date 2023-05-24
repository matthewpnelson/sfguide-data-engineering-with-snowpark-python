import sys

def main(gas_net_mcf: float, ngl_yield_factor: float, work_interest: float) -> float:

    return 0 if work_interest == 0 else ((gas_net_mcf / 1000) * ngl_yield_factor) / (work_interest * 0.01)


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore
