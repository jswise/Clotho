import pandas as pd


def test_get(victim):
    df = victim.get('Fruit')
    assert len(df) > 1

    df = victim.get('Fruit', """ "Name" == 'banana' """)
    assert len(df) == 1


def test_import_df(victim):
    df = victim.get('Fruit')
    fruit_id = df.FruitID.max() + 1
    addendum = pd.DataFrame([{'FruitID': fruit_id, 'Name': 'tomato', 'Color': 'red'}])
    df = pd.concat([df, addendum])
    victim.import_df(df, 'Fruit', True)
    df = victim.get('Fruit', """ "FruitID" == {} """.format(fruit_id))
    assert df['Name'].iloc[0] == 'tomato'


def test_table_names(victim):
    table_names = victim.table_names
    assert 'Fruit' in table_names
