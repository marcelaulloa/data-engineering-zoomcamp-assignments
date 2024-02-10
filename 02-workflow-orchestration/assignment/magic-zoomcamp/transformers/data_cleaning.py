import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test




@transformer
def transform(data, *args, **kwargs):
    print("Preprocessing: rows with zero passangers:", data['passenger_count'].isin([0]).sum())
    data = data[data['passenger_count']>0]

    print("Preprocessing: rows with trip distance 0:", data['trip_distance'].isin([0]).sum())
    data = data[data['trip_distance']>0]  
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    
    # Function to convert CamelCase to snake_case
    def camel_to_snake(name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

    # Rename columns
    old_columnms = data.columns
    data= data.rename(columns=camel_to_snake)

    changes = 0
    for old,new in zip(old_columnms,data.columns):
        if old != new:
            changes +=1
    print("Number of columns changed: "+str(changes))

    unique_values = data['vendor_id'].unique()
    print(unique_values)
    print(data['lpep_pickup_date'].unique())


    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # assert output is not No ne, 'The output is undefined'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zeros passangers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with distance equal to zero'
    assert 'vendor_id' in output.columns, "The column 'vendor_id' does not exist in the DataFrame"


