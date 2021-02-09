#pragma once

#include <type_traits>
#include <experimental/type_traits>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <common/logger_useful.h>


namespace DB
{
template <typename T>
using DecimalOrVectorCol = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

template <typename T>
struct AggregationFunctionDeltaSumData
{
    T sum = 0;
    T last = 0;
    T first = 0;
    bool seen_last = false;
    bool seen_first = false;
};

template <typename T>
class AggregationFunctionDeltaSum final
    : public IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>
{
public:
    AggregationFunctionDeltaSum(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>{arguments, params}
    {}

    AggregationFunctionDeltaSum()
        : IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>{}
    {}

    String getName() const override { return "deltaSum"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = assert_cast<const DecimalOrVectorCol<T> &>(*columns[0]).getData()[row_num];

        if ((this->data(place).last < value) && this->data(place).seen_last)
        {
            this->data(place).sum += (value - this->data(place).last);
        }

        this->data(place).last = value;
        this->data(place).seen_last = true;

        if (!this->data(place).seen_first)
        {
            this->data(place).first = value;
            this->data(place).seen_first = true;
        }
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        static int id_gen = 0;
        id_gen++;

        int id = id_gen;

        auto place_data = &this->data(place);
        auto rhs_data = &this->data(rhs);

        printf("\n\n**************** merge\n");
        LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. our seen last: {} our seen first: {}", toString(id), toString(this->data(place).seen_last), toString(this->data(place).seen_first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. our last: {} our first: {}", toString(id), toString(this->data(place).last), toString(this->data(place).first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. their seen last: {} their seen first: {}", toString(id), toString(this->data(rhs).seen_last), toString(this->data(rhs).seen_first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. their last: {} their first: {}", toString(id), toString(this->data(rhs).last), toString(this->data(rhs).first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. our sum: {} their sum: {}", toString(id), toString(this->data(place).sum), toString(this->data(rhs).sum));

        if ((place_data->last < rhs_data->first) && place_data->seen_last && rhs_data->seen_first)
        {
            // If the lhs last number seen is less than the first number the rhs saw, the lhs is before
            // the rhs, for example [0, 2] [4, 7]. So we want to add the deltasums, but also add the
            // difference between lhs last number and rhs first number (the 2 and 4). Then we want to
            // take last value from the rhs, so first and last become 0 and 7.

            place_data->sum += rhs_data->sum + (rhs_data->first - place_data->last);
            place_data->last = rhs_data->last;

            LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. our last is less then their first; adding that delta as well. Our sum now {}",
                toString(id), toString(this->data(place).sum));
        }
        else if ((rhs_data->last < place_data->first && rhs_data->seen_last && place_data->seen_first))
        {
            // In the opposite scenario, the lhs comes after the rhs, e.g. [4, 6] [1, 2]. Since we
            // assume the input interval states are sorted by time, we assume this is a counter
            // reset, and therefore do *not* add the difference between our first value and the 
            // rhs last value.

            place_data->sum += rhs_data->sum;
            place_data->first = rhs_data->first;

            LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. their last is less then our first; adding that delta as well. Our sum now {}",
                toString(id), toString(this->data(place).sum));
        }
        else if (rhs_data->seen_first)
        {
            // If we're here then the lhs is an empty state and the rhs does have some state, so
            // we'll just take that state.

            place_data->first = rhs_data->first;
            place_data->seen_first = rhs_data->seen_first;
            place_data->last = rhs_data->last;
            place_data->seen_last = rhs_data->seen_last;
            place_data->sum = rhs_data->sum;

            LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. one interval is empty; taking rhs, {}",
                toString(id), toString(this->data(place).sum));
        }

        LOG_INFO(&Poco::Logger::get("Aggregator"), "{}. post merge sum is now {}",
            toString(id), toString(this->data(place).sum));

        // Otherwise lhs either has data or is unitialized, so we don't need to modify its values.
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeIntBinary(this->data(place).sum, buf);
        writeIntBinary(this->data(place).first, buf);
        writeIntBinary(this->data(place).last, buf);
        writePODBinary<bool>(this->data(place).seen_first, buf);
        writePODBinary<bool>(this->data(place).seen_last, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readIntBinary(this->data(place).sum, buf);
        readIntBinary(this->data(place).first, buf);
        readIntBinary(this->data(place).last, buf);
        readPODBinary<bool>(this->data(place).seen_first, buf);
        readPODBinary<bool>(this->data(place).seen_last, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).sum);
    }
};

}
