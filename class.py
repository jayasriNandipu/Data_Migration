# method1:-
class param:
        """Learning coading---"""
        age=10
        print(age)

object=param()
print(object.age)
print(param.age)
print(object.__doc__)


# method:2-----
class param:
    """learning coading---"""
    age=10
    print(age)
    def fun(self):
        "param institute"
        # (you want to print param institute you must call and
        # you write this after the def function and call it in this way only
        # print(obj.fun.__doc__))
        # (if you eant to print string by using print(obj.fun.__doc__))
        name="learn coading"
        print(name)
obj=param()
obj.fun()
print(obj.fun.__doc__)
print(obj.age)
print(param.age)



# method:3------
class A:
    def __init__(self,age,name,address):
        print(age," ",name," ",address)
obj=A(10,"Ankith","Jan")
obj=A(10,"param","guduru")
obj=A("athma",100,200)