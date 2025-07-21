import string
import random



""" ***********************************************************
제작 : (주)와토시스 김진욱
종류 : python
컴파일 : -
사용 :  유틸리티성 기능 
최초작성일 : 2023, 02, 15
최종수정일 : 2024, 06, 01
주의점 : 
************************************************************ """


class GBUtil:
    def __init__(self):
        self.files = []


    """
    nformat 
        0 : Only Digit
        1 : Only Alphabat
        2 : Digit & Alpha
        3 : Digit & Alpha * Special
    """
    @classmethod
    def get_random_key(self, nMode, length=32):
        if nMode == 0:
            characters = string.digits
        elif nMode == 1:
            characters = string.ascii_letters
        elif nMode == 2:
            characters = string.ascii_letters + string.digits
        elif nMode == 3:
            characters = string.ascii_letters + string.digits + string.punctuation

        random_string = ''.join(random.choice(characters) for _ in range(length))
        
        return random_string
      
    @classmethod
    def generate_padded_number(self, invalue, length, zerofill=True):
        number_str = str(invalue)
        if len(number_str) >= length:
            return number_str
        else:
            if zerofill is False:
                padding = ''.join(str(random.randint(0, 9)) for _ in range(length - len(number_str)))
            else:
                padding = ''.join(str(0) for _ in range(length - len(number_str)))

            return padding + number_str


    def compare_dicts(self, dict1, dict2):
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())
        
        common_keys = keys1.intersection(keys2)
        only_in_dict1 = keys1 - keys2
        only_in_dict2 = keys2 - keys1
        
        return {
            'common_keys': common_keys,
            'only_in_dict1': only_in_dict1,
            'only_in_dict2': only_in_dict2
        }


    def diff_dict(self, array1, array2):
        diff_keys = set(dict2.keys()) - set(dict1.keys())

        return diff_keys        

    def filter_unique_array(self, array1, array2):
        data_type = type(array1)

        if data_type is list:
            # array1에는 있지만 array2에는 없는 항목을 필터링
            unique_items = [item for item in array1 if item not in array2]
        elif data_type is dict:
            unique_items = {key: value for key, value in dict1.items() if key not in dict2}

        return unique_items

    def transform_variable_name(self, variable_name):
        # Remove underscores and capitalize the first letter of each word
        return ''.join(word.capitalize() for word in variable_name.split('_'))

    def camel_to_snake(variable_name):
        snake_case = ''
        for char in variable_name:
            if char.isupper():
                snake_case += '_' + char.lower()
            else:
                snake_case += char
        return snake_case.lstrip('_')

    # camel_to_snake.py
    def camel_to_snake(self, name):
        import re
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).upper() #.upper() .lower()

    # snake_to_camel.py
    def snake_to_camel(self, name):
        components = name.split('_')
        return components[0] + ''.join(x.title() for x in components[1:])

    def tocamel_dictname(self, vector):
        #vector = {'firstName': 'John', 'lastName': 'Doe', 'emailAddress': 'john.doe@example.com'}
        camel_case_vector = {self.snake_to_camel(key): value for key, value in vector.items()}

        return camel_case_vector


    def tosnake_dictname(self, vector):
        #vector = {'firstName': 'John', 'lastName': 'Doe', 'emailAddress': 'john.doe@example.com'}
        snake_case_vector = {self.camel_to_snake(key): value for key, value in vector.items()}

        return snake_case_vector

    def dictToSql(self, pdict_data):
        db_in_datas = {}
        for key,val in pdict_data.items():
            if val != '':
                db_in_datas["\""+key+"\""] = str(val)

        return db_in_datas

    #두 지점간에 선형위치값 N개 추가하기
    """
    # Example usage
    lat1, lon1 = 34.0522, -118.2437  # Los Angeles
    lat2, lon2 = 36.1699, -115.1398  # Las Vegas
    N = 5

    positions = interpolate_coordinates(lat1, lon1, lat2, lon2, N)
    print(positions)
    """
    def interpolate_coordinates(self, lat1, lon1, lat2, lon2, N):
        lat_values = [lat1 + (lat2 - lat1) * i / (N + 1) for i in range(1, N + 1)]
        lon_values = [lon1 + (lon2 - lon1) * i / (N + 1) for i in range(1, N + 1)]

        return list(zip(lat_values, lon_values))

    # 두 개의 경도, 위도 간의 거리계산하고 10km 속도로 움직일 때 소요시간 계산 
    def coordinates_distance(lat1, lon1, lat2, lon2, N):
        from geopy.distance import great_circle

        # Coordinates of two locations (latitude, longitude)
        location1 = (latitude1, longitude1)  # Replace with actual coordinates
        location2 = (latitude2, longitude2)  # Replace with actual coordinates

        # Calculate distance in kilometers
        distance = great_circle(location1, location2).kilometers

        # Speed in km/h
        speed = 10

        # Calculate time in hours
        time = distance / speed

        # Output the results
        print(f"Distance: {distance:.2f} km")
        print(f"Time to travel at {speed} km/h: {time:.2f} hours")

        return (distance, time)

if __name__ == "__main__":
    fbfile = GBUtil()

    strdate = GBUtil.get_random_key(0)
    print(strdate)

    strdate = GBUtil.get_random_key(1)
    print(strdate)

    strdate = GBUtil.get_random_key(2)
    print(strdate)

    strdate = GBUtil.get_random_key(3)
    print(strdate)

    strdate = GBUtil.get_random_key(3, 16)
    print(strdate)

    strdate = GBUtil.get_random_key(2, 64)
    print(strdate)

    # 예시 배열
    array1 = [1, 2, 3, 4, 5]
    array2 = [4, 5, 6, 7, 8, 9, 10]

    # 함수 호출
    result = fbfile.filter_unique_array(array1, array2)
    print(result) 

    # 예시 딕셔너리
    dict1 = {'a': 1, 'b': 2, 'c': 3}
    dict2 = {'b': 2, 'd': 4}

    # 함수 호출
    result = fbfile.filter_unique_array(dict1, dict2)
    print("===================================================") 
    print(result)


    # 예시 딕셔너리
    dict1 = {'a': ['a1', 'a2'], 'b': ['b1', 'b2'], 'c': ['c1', 'c2'], 'd': ['d1', 'd2'] }
    dict2 = {'c': ['c1', 'c3'], 'd': ['d1', 'd2', 'd3'], 'e':['a1', 'a2'], 'f':['a1', 'a2']}

    dict1 =  {'/client_count': {'topic_nm': '/client_count'}, '/connected_clients': {'topic_nm': '/connected_clients'}, '/rosout': {'topic_nm': '/rosout'}, '/rosout_agg': {'topic_nm': '/rosout_agg'}}
    dict2 =  {'/connected_clients': {'topic_nm': '/connected_clients'}, '/rosout': {'topic_nm': '/rosout'}, '/rosout_agg': {'topic_nm': '/rosout_agg'}, '/rosout_agg2': {'topic_nm': '/rosout_agg'}}

    print(dict1.keys())
    print(dict2.keys())
    diff_keys = fbfile.compare_dicts(dict1, dict2)
    print("Keys in dict2 not in dict1:", diff_keys)

    result = fbfile.filter_unique_array(dict1, dict2)
    print("===================================================") 
    print(result) 
    

    variable = "example_variable_name"
    transformed_variable = fbfile.snake_to_camel(variable)
    print("===================================================") 
    print(transformed_variable)  # Output: ExampleVariableName
    

    transformed_variable = fbfile.camel_to_snake(transformed_variable)
    print("===================================================") 
    print(transformed_variable)

    vector = {'firstName': 'John', 'lastName': 'Doe', 'emailAddress': 'john.doe@example.com'}
    snake_case_vector = fbfile.tosnake_dictname(vector)
    print(snake_case_vector)
    

    snake_case_vector = fbfile.tocamel_dictname(snake_case_vector)
    print(snake_case_vector)

    snake_case_vector = fbfile.tosnake_dictname(snake_case_vector)
    print(snake_case_vector)


    
