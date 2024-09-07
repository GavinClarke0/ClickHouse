#include "config.h"

#if USE_S2_GEOMETRY

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include "s2_fwd.h"

class S2CellId;

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/**
 * Accepts points of the form (longitude, latitude)
 * Returns s2 identifier
 */
class FunctionS2CellParent : public IFunction
{
public:
    static constexpr auto name = "s2CellParent";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionS2CellParent>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const auto * arg = arguments[i].get();
            if (!WhichDataType(arg).isFloat64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {} of function {}. Must be Float64",
                    arg->getName(), i, getName());
        }

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_cell_id = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_lon)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data_col_cell_id = col_cell_id->getData();

        const auto * col_level = checkAndGetColumn<ColumnUInt64>(non_const_arguments[1].column.get());
        if (!col_lat)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arguments[0].type->getName(),
                2,
                getName());
        const auto & data_col_level = col_level->getData();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 cell_id = S2CellId(data_col_cell_id[row]);
            const UInt64 level = data_col_level[row];

            if (!cell_id.is_valid())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "CellId is invalid.");

            if (level >= 30)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Level must be equal or lessthen 30.");


            S2CellId parent_cell_id  = cell_id.parent(level)

            dst_data[row] = parent_cell_id.id();
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(S2CellParent)
{
    factory.registerFunction<FunctionS2CellParent>();
}

}

#endif
