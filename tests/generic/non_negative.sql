-- Значение в колонке не меньше нуля.
-- Возвращает строки, где значение отрицательное. 0 строк — тест пройден.
{% test non_negative(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
