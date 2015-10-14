#pragma once

#include <DB/Core/Field.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBufferFromString.h>


namespace DB
{


/** StaticVisitor (его наследники) - класс с перегруженными для разных типов операторами ().
  * Вызвать visitor для field можно с помощью функции apply_visitor.
  * Также поддерживается visitor, в котором оператор () принимает два аргумента.
  */
template <typename R = void>
struct StaticVisitor
{
	typedef R ResultType;
};


template <typename Visitor, typename F>
typename Visitor::ResultType apply_visitor_impl(Visitor & visitor, F & field)
{
	switch (field.getType())
	{
		case Field::Types::Null: 				return visitor(field.template get<Null>());
		case Field::Types::UInt64: 				return visitor(field.template get<UInt64>());
		case Field::Types::Int64: 				return visitor(field.template get<Int64>());
		case Field::Types::Float64: 			return visitor(field.template get<Float64>());
		case Field::Types::String: 				return visitor(field.template get<String>());
		case Field::Types::Array: 				return visitor(field.template get<Array>());

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

/** Эти штуки нужны, чтобы принимать временный объект по константной ссылке.
  * В шаблон выше, типы форвардятся уже с const-ом.
  */

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, Field & field)
{
	return apply_visitor_impl(visitor, field);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, const Field & field)
{
	return apply_visitor_impl(visitor, field);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, Field & field)
{
	return apply_visitor_impl(visitor, field);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, const Field & field)
{
	return apply_visitor_impl(visitor, field);
}


template <typename Visitor, typename F1, typename F2>
typename Visitor::ResultType apply_binary_visitor_impl2(Visitor & visitor, F1 & field1, F2 & field2)
{
	switch (field2.getType())
	{
		case Field::Types::Null: 				return visitor(field1, field2.template get<Null>());
		case Field::Types::UInt64: 				return visitor(field1, field2.template get<UInt64>());
		case Field::Types::Int64: 				return visitor(field1, field2.template get<Int64>());
		case Field::Types::Float64: 			return visitor(field1, field2.template get<Float64>());
		case Field::Types::String: 				return visitor(field1, field2.template get<String>());
		case Field::Types::Array: 				return visitor(field1, field2.template get<Array>());

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

template <typename Visitor, typename F1, typename F2>
typename Visitor::ResultType apply_binary_visitor_impl1(Visitor & visitor, F1 & field1, F2 & field2)
{
	switch (field1.getType())
	{
		case Field::Types::Null: 				return apply_binary_visitor_impl2(visitor, field1.template get<Null>(), 	field2);
		case Field::Types::UInt64: 				return apply_binary_visitor_impl2(visitor, field1.template get<UInt64>(), 	field2);
		case Field::Types::Int64: 				return apply_binary_visitor_impl2(visitor, field1.template get<Int64>(), 	field2);
		case Field::Types::Float64: 			return apply_binary_visitor_impl2(visitor, field1.template get<Float64>(), 	field2);
		case Field::Types::String: 				return apply_binary_visitor_impl2(visitor, field1.template get<String>(), 	field2);
		case Field::Types::Array: 				return apply_binary_visitor_impl2(visitor, field1.template get<Array>(), field2);

		default:
			throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
	}
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, const Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(Visitor & visitor, const Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, const Field & field1, Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}

template <typename Visitor>
typename Visitor::ResultType apply_visitor(const Visitor & visitor, const Field & field1, const Field & field2)
{
	return apply_binary_visitor_impl1(visitor, field1, field2);
}


/** Возвращает строковый дамп типа */
class FieldVisitorDump : public StaticVisitor<String>
{
private:
	template <typename T>
	static inline String formatQuotedWithPrefix(T x, const char * prefix)
	{
		String res;
		WriteBufferFromString wb(res);
		wb.write(prefix, strlen(prefix));
		writeQuoted(x, wb);
		return res;
	}
public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return formatQuotedWithPrefix(x, "UInt64_"); }
	String operator() (const Int64 		& x) const { return formatQuotedWithPrefix(x, "Int64_"); }
	String operator() (const Float64 	& x) const { return formatQuotedWithPrefix(x, "Float64_"); }
	String operator() (const String 	& x) const;
	String operator() (const Array 		& x) const;
};


/** Выводит текстовое представление типа, как литерала в SQL запросе */
class FieldVisitorToString : public StaticVisitor<String>
{
private:
	template <typename T>
	static inline String formatQuoted(T x)
	{
		String res;
		WriteBufferFromString wb(res);
		writeQuoted(x, wb);
		return res;
	}

	/** В отличие от writeFloatText (и writeQuoted), если число после форматирования выглядит целым, всё равно добавляет десятичную точку.
	  * - для того, чтобы это число могло обратно распарситься как Float64 парсером запроса (иначе распарсится как целое).
	  *
	  * При этом, не оставляет завершающие нули справа.
	  *
	  * NOTE: При таком roundtrip-е, точность может теряться.
	  */
	static String formatFloat(const Float64 x);

public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return formatQuoted(x); }
	String operator() (const Int64 		& x) const { return formatQuoted(x); }
	String operator() (const Float64 	& x) const { return formatFloat(x); }
	String operator() (const String 	& x) const { return formatQuoted(x); }
	String operator() (const Array 		& x) const;
};


/** Числовой тип преобразует в указанный. */
template <typename T>
class FieldVisitorConvertToNumber : public StaticVisitor<T>
{
public:
	T operator() (const Null & x) const
	{
		throw Exception("Cannot convert NULL to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}

	T operator() (const String & x) const
	{
		throw Exception("Cannot convert String to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}

	T operator() (const Array & x) const
	{
		throw Exception("Cannot convert Array to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
	}

	T operator() (const UInt64 	& x) const { return x; }
	T operator() (const Int64 	& x) const { return x; }
	T operator() (const Float64 & x) const { return x; }
};


/// Преобразование строки с датой или датой-с-временем в UInt64, содержащим числовое значение даты или даты-с-временем.
UInt64 stringToDateOrDateTime(const String & s);


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

/** Более точное сравнение.
  * Отличается от Field::operator< и Field::operator== тем, что сравнивает значения разных числовых типов между собой.
  * Правила сравнения - такие же, что и в FunctionsComparison.
  * В том числе, сравнение знаковых и беззнаковых оставляем UB.
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
	bool operator() (const Null & l, const Null & r)        const { return true; }
	bool operator() (const Null & l, const UInt64 & r)      const { return false; }
	bool operator() (const Null & l, const Int64 & r)       const { return false; }
	bool operator() (const Null & l, const Float64 & r)     const { return false; }
	bool operator() (const Null & l, const String & r)      const { return false; }
	bool operator() (const Null & l, const Array & r)       const { return false; }

	bool operator() (const UInt64 & l, const Null & r)      const { return false; }
	bool operator() (const UInt64 & l, const UInt64 & r)    const { return l == r; }
	bool operator() (const UInt64 & l, const Int64 & r)     const { return l == r; }
	bool operator() (const UInt64 & l, const Float64 & r)   const { return l == r; }
	bool operator() (const UInt64 & l, const String & r)    const { return l == stringToDateOrDateTime(r); }
	bool operator() (const UInt64 & l, const Array & r)     const { return false; }

	bool operator() (const Int64 & l, const Null & r)       const { return false; }
	bool operator() (const Int64 & l, const UInt64 & r)     const { return l == r; }
	bool operator() (const Int64 & l, const Int64 & r)      const { return l == r; }
	bool operator() (const Int64 & l, const Float64 & r)    const { return l == r; }
	bool operator() (const Int64 & l, const String & r)     const { return false; }
	bool operator() (const Int64 & l, const Array & r)      const { return false; }

	bool operator() (const Float64 & l, const Null & r)     const { return false; }
	bool operator() (const Float64 & l, const UInt64 & r)   const { return l == r; }
	bool operator() (const Float64 & l, const Int64 & r)    const { return l == r; }
	bool operator() (const Float64 & l, const Float64 & r)  const { return l == r; }
	bool operator() (const Float64 & l, const String & r)   const { return false; }
	bool operator() (const Float64 & l, const Array & r)    const { return false; }

	bool operator() (const String & l, const Null & r)      const { return false; }
	bool operator() (const String & l, const UInt64 & r)    const { return stringToDateOrDateTime(l) == r; }
	bool operator() (const String & l, const Int64 & r)     const { return false; }
	bool operator() (const String & l, const Float64 & r)   const { return false; }
	bool operator() (const String & l, const String & r)    const { return l == r; }
	bool operator() (const String & l, const Array & r)     const { return false; }

	bool operator() (const Array & l, const Null & r)       const { return false; }
	bool operator() (const Array & l, const UInt64 & r)     const { return false; }
	bool operator() (const Array & l, const Int64 & r)      const { return false; }
	bool operator() (const Array & l, const Float64 & r)    const { return false; }
	bool operator() (const Array & l, const String & r)     const { return false; }
	bool operator() (const Array & l, const Array & r)      const { return l == r; }
};

class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
	bool operator() (const Null & l, const Null & r)        const { return false; }
	bool operator() (const Null & l, const UInt64 & r)      const { return true; }
	bool operator() (const Null & l, const Int64 & r)       const { return true; }
	bool operator() (const Null & l, const Float64 & r)     const { return true; }
	bool operator() (const Null & l, const String & r)      const { return true; }
	bool operator() (const Null & l, const Array & r)       const { return true; }

	bool operator() (const UInt64 & l, const Null & r)      const { return false; }
	bool operator() (const UInt64 & l, const UInt64 & r)    const { return l < r; }
	bool operator() (const UInt64 & l, const Int64 & r)     const { return l < r; }
	bool operator() (const UInt64 & l, const Float64 & r)   const { return l < r; }
	bool operator() (const UInt64 & l, const String & r)    const { return l < stringToDateOrDateTime(r); }
	bool operator() (const UInt64 & l, const Array & r)     const { return true; }

	bool operator() (const Int64 & l, const Null & r)       const { return false; }
	bool operator() (const Int64 & l, const UInt64 & r)     const { return l < r; }
	bool operator() (const Int64 & l, const Int64 & r)      const { return l < r; }
	bool operator() (const Int64 & l, const Float64 & r)    const { return l < r; }
	bool operator() (const Int64 & l, const String & r)     const { return true; }
	bool operator() (const Int64 & l, const Array & r)      const { return true; }

	bool operator() (const Float64 & l, const Null & r)     const { return false; }
	bool operator() (const Float64 & l, const UInt64 & r)   const { return l < r; }
	bool operator() (const Float64 & l, const Int64 & r)    const { return l < r; }
	bool operator() (const Float64 & l, const Float64 & r)  const { return l < r; }
	bool operator() (const Float64 & l, const String & r)   const { return true; }
	bool operator() (const Float64 & l, const Array & r)    const { return true; }

	bool operator() (const String & l, const Null & r)      const { return false; }
	bool operator() (const String & l, const UInt64 & r)    const { return stringToDateOrDateTime(l) < r; }
	bool operator() (const String & l, const Int64 & r)     const { return false; }
	bool operator() (const String & l, const Float64 & r)   const { return false; }
	bool operator() (const String & l, const String & r)    const { return l < r; }
	bool operator() (const String & l, const Array & r)     const { return true; }

	bool operator() (const Array & l, const Null & r)       const { return false; }
	bool operator() (const Array & l, const UInt64 & r)     const { return false; }
	bool operator() (const Array & l, const Int64 & r)      const { return false; }
	bool operator() (const Array & l, const Float64 & r)    const { return false; }
	bool operator() (const Array & l, const String & r)     const { return false; }
	bool operator() (const Array & l, const Array & r)      const { return l < r; }
};

#pragma GCC diagnostic pop

}
