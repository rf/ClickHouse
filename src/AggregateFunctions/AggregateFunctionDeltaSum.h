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
struct AggregationFunctionDeltaSumData
{
    T sum = 0;
    bool seen_last = false;
    T last = 0;
    bool seen_first = false;
    T first = 0;
};

template <typename T>
class AggregationFunctionDeltaSum final
    : public IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>
{
public:
    AggregationFunctionDeltaSum(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>{arguments, params}
    {
        // empty constructor
    }

    String getName() const override { return "deltaSum"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    void ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];

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

    void ALWAYS_INLINE merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        printf("\n\n**************** merge\n");
        LOG_INFO(&Poco::Logger::get("Aggregator"), "our seen last: {} our seen first: {}", toString(this->data(place).seen_last), toString(this->data(place).seen_first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "our last: {} our first: {}", toString(this->data(place).last), toString(this->data(place).first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "their seen last: {} their seen first: {}", toString(this->data(rhs).seen_last), toString(this->data(rhs).seen_first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "their last: {} their first: {}", toString(this->data(rhs).last), toString(this->data(rhs).first));
        LOG_INFO(&Poco::Logger::get("Aggregator"), "our sum: {} their sum: {}", toString(this->data(place).sum), toString(this->data(rhs).sum));

        printf("\n\n\n\n\n");

        if ((this->data(place).last < this->data(rhs).first) && this->data(place).seen_last && this->data(rhs).seen_first)
        {
            this->data(place).sum += this->data(rhs).sum + (this->data(rhs).first - this->data(place).last);
            LOG_INFO(&Poco::Logger::get("Aggregator"), "our last is les then their first; adding that delta as well. Our sum now {}",
                    toString(this->data(place).sum));
            this->data(place).last = this->data(rhs).last;
        }
        else if ((this->data(rhs).last < this->data(place).first && this->data(rhs).seen_last && this->data(place).seen_first))
        {
            this->data(place).sum += this->data(rhs).sum + (this->data(place).first - this->data(rhs).last);
            LOG_INFO(&Poco::Logger::get("Aggregator"), "their last is les then our first; adding that delta as well. Our sum now {}",
                    toString(this->data(place).sum));
            this->data(place).first = this->data(rhs).first;
        }
        else
        {
            this->data(place).sum += this->data(rhs).sum;
            LOG_INFO(&Poco::Logger::get("Aggregator"), "Simply adding the sums; our sum is now {}", toString(this->data(place).sum));
            this->data(place).first = this->data(rhs).first;
            this->data(place).seen_first = this->data(rhs).seen_first;
            this->data(place).last = this->data(rhs).last;
            this->data(place).seen_last = this->data(rhs).seen_last;
        }
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
