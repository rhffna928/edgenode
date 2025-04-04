from enum import Enum

""" ***********************************************************
제작 : (주)와토시스 김진욱
종류 : python
컴파일 : -
사용 :  ENUM, STRUCTURE, 상수 
최초작성일 : 2023, 02, 15
최종수정일 : 2024, 06, 01
주의점 : 
************************************************************ """


class Constant(Enum):
    NONE = 0
    EDGENODE = 1
    EDGEHUB = 2
    EDGE = 3
    MOBILESOCK1 = 4
    MOBILESOCK2 = 5
    EDGE_EDGEHUB = 6
