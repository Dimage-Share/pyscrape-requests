from app import create_app


app = create_app({
    'SECRET_KEY': 'dev'
})
with app.test_client() as c:
    rv = c.get('/')
    print('STATUS', rv.status_code)
    data = rv.get_data(as_text=True)
    print(data[:2000])
