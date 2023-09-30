from pprint import pprint
from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result
    


def select_2(discipline_id: int):
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return r
    

def select_3(discipline_id: int):
    '''
    Знайти середній бал у групах з певного предмета.
    '''
    r = session.query(Discipline.name,
                      Group.name,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      )\
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .join(Group)\
        .filter(Discipline.id == discipline_id)\
        .group_by(Group.name, Discipline.name)\
        .order_by(desc('avg_grade'))\
        .all()
    return r


def select_4():
    '''
    Знайти середній бал на потоці (по всій таблиці оцінок).
    '''
    r = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')).select_from(Grade).all()
    return r


def select_5(teacher_id: int):
    '''
    Знайти які курси читає певний викладач.
    '''
    r = session.query(Discipline.name, Teacher.fullname)\
        .select_from(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == teacher_id)\
        .group_by(Discipline.name, Teacher.fullname).all()
    return r


def select_6(group_id: int):
    '''
    Знайти список студентів у певній групі.
    '''
    r = session.query(Student.fullname, Group.name)\
        .select_from(Student)\
        .join(Group)\
        .filter(Group.id == group_id)\
        .group_by(Student.fullname, Group.name).all()
    return r


def select_7(group_id, disciplines_id):
    '''
    Знайти оцінки студентів у окремій групі з певного предмета.
    '''
    r = session.query(Student.fullname, Group.name, Discipline.name, Grade.grade)\
        .select_from(Student)\
        .join(Grade)\
        .join(Discipline)\
        .join(Group)\
        .filter(and_(Group.id == group_id, Discipline.id == disciplines_id))\
        .group_by(Student.fullname, Group.name, Discipline.name, Grade.grade).all()
    return r


def select_8(teacher_id: int):
    '''
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    '''
    r = session.query(Teacher.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade)\
        .join(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == teacher_id)\
        .group_by(Teacher.fullname, Discipline.name)\
        .order_by(desc('avg_grade')).all()
    return r


def select_9(students_id: int):
    '''
    Знайти список курсів, які відвідує певний студент.
    '''
    r = session.query(Student.fullname, Discipline.name)\
        .select_from(Discipline)\
        .join(Grade)\
        .join(Student)\
        .filter(Student.id == students_id)\
        .group_by(Student.fullname, Discipline.name).all()
    return r


def select_10(students_id, teacher_id):
    '''
    Список курсів, які певному студенту читає певний викладач.
    '''
    r = session.query(Discipline.name, Student.fullname, Teacher.fullname)\
        .select_from(Discipline)\
        .join(Grade)\
        .join(Student)\
        .join(Teacher)\
        .filter(and_(Student.id == students_id, Teacher.id == teacher_id))\
        .group_by(Discipline.name, Student.fullname, Teacher.fullname).all()
    return r
        




if __name__ == '__main__':
    # pprint(select_1())
    # pprint(select_2(2))
    # pprint(select_3(3))
    # pprint(select_4())
    # pprint(select_5(3))
    # pprint(select_6(1))
    # pprint(select_7(1, 5))
    # pprint(select_8(5))
    # pprint(select_9(1))
    pprint(select_10(7, 5))
