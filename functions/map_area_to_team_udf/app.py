import sys

def main(area: str) -> str:

    mapping = {'LAPRISE': 'PRA', 'CAROLINE': 'WILDCAT', 'WOLVERINE': 'PRA', 'P1P2': 'EAST', 'DROWNINGFD': 'EAST', 'FERRIER': 'CENTRAL', 'WATELET': 'CENTRAL', 'CHANNEL LK': 'EAST', 'STOLBERG': 'EDSON', 'CABIN': 'PRA', 'YOYO': 'PRA', 'REILLY': 'EDSON', 'BANSHEE': 'EDSON', 'SHAW': 'EDSON', 'WILSON CRK': 'CENTRAL', 'VIKING': 'WILDCAT', 'BURSTALL': 'EAST', 'BOUNDARY': 'PRA', 'VOYAGER': 'EDSON', 'CLARKE LK': 'PRA', 'HANLANNOUN': 'EDSON', 'VALE': 'EAST', 'MILO': 'PRA', 'MINFTSTJHN': 'PRA', 'SCHULER': 'EAST', 'MINGRPRAIR': 'PRA', 'STOLBERGUN': 'EDSON', 'REDCAP': 'EDSON', 'TURNER VAL': 'WILDCAT', 'CORDEL': 'EDSON', 'BIGSTICK': 'EAST', 'MTN PARK': 'EDSON', 'BURNT TIMB': 'WILDCAT', 'ARCHER': 'PRA', 'BOUGIE': 'PRA', 'HILDA': 'EAST', 'MEDALLION': 'WILDCAT', 'WCH UNIT': 'WILDCAT', 'KLUA': 'PRA', 'ALDERSON': 'EAST', 'BENJAMIN': 'WILDCAT', 'BIRCH': 'EAST', 'HUNTER VAL': 'WILDCAT', 'CRANE LK': 'EAST', 'HANLANUN': 'EDSON', 'MHCU1': 'EAST', 'GRANLEA': 'EAST', 'MED LODGE': 'EDSON', 'GHOST RIV': 'WILDCAT', 'PANTHER': 'WILDCAT', 'GWILLIM': 'PRA', 'THORSBY': 'EAST', 'GILBY': 'CENTRAL', 'MINN BL UN': 'CENTRAL', 'PECO': 'EDSON', 'FERRIERNC': 'CENTRAL', 'NESTOW': 'EAST', 'COMMOTION': 'PRA', 'BASING': 'EDSON', 'BLACKBUTTE': 'EAST', 'MINFTNELSN': 'PRA', 'BROWN CRK': 'EDSON', 'SWBRNTTIMB': 'WILDCAT', 'BLACKSTONE': 'EDSON', 'MUSKIKI': 'EDSON', 'PEMBINA': 'EAST', 'PARKLAND': 'PRA', None: None, 'OJAY': 'PRA', 'GLACIER': 'PRA'}
    
    mapping['BASHAW'] = 'EAST'
    
    return mapping[area]


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore
