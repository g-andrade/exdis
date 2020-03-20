defmodule Exdis.Database.String do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Constants
  ## ------------------------------------------------------------------

  @min_integer_value (-(1 <<< 63))
  @max_integer_value ((1 <<< 63) - 1)

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:string,
    repr: :binary,
    value: nil
  )

  ## ------------------------------------------------------------------
  ## Utilities
  ## ------------------------------------------------------------------

  def min_integer_value(), do: @min_integer_value
  def max_integer_value(), do: @max_integer_value

  def max_integer_value_str_length() do
    # TODO evaluate this at compile time using `ct_transform` equivalent?
    str_min = Integer.to_string(min_integer_value())
    str_max = Integer.to_string(max_integer_value())
    max(byte_size(str_min), byte_size(str_max) + 1) # +1 for an optional plus sign
  end

  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append(key, tail, reply_cb) do
    Exdis.Database.KeyOwner.upsert(key,
      &handle_append(&1, tail),
      fn -> handle_init({:integer, byte_size(tail)}, :binary, tail) end,
      reply_cb)
  end

  defp handle_append(string() = state, tail) do
    case coerce_into_binary_or_iodata(state) do
      string(repr: :binary, value: value) = state ->
        retval = {:integer, byte_size(value) + byte_size(tail)}
        value = [value, tail]
        state = string(state, repr: {:iodata, 1}, value: value)
        {retval, state}
      string(repr: {:iodata, depth}, value: value) = state ->
        value = [value, tail]
        retval = {:integer, :erlang.iolist_size(value)}
        state = string(state, repr: {:iodata, depth + 1}, value: value)
        {retval, state}
    end
  end

  defp handle_append(_state, _tail) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GET Command
  ## ------------------------------------------------------------------

  def get(key, reply_cb) do
    Exdis.Database.KeyOwner.read(key, &handle_get/1, nil, reply_cb)
  end

  defp handle_get(string() = state) do
    state = flatten_iodata(state)
    string(value: value) = coerce_into_binary(state)
    {{:string, value}, state}
  end

  defp handle_get(state) when not Record.is_record(state, :string) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by(key, increment, reply_cb) do
    Exdis.Database.KeyOwner.upsert(key,
      &handle_increment_by(&1, increment),
      fn -> handle_init({:integer, increment}, :integer, increment) end,
      reply_cb)
  end

  defp handle_increment_by(string() = state, increment) do
    case maybe_coerce_into_integer(state) do
      string(value: value) = state ->
        case value + increment do
          value when value >= @min_integer_value and value <= @max_integer_value ->
            retval = {:integer, value}
            state = string(state, value: value)
            {retval, state}
          _underflow_or_overflow ->
            retval = {:error, :increment_or_decrement_would_overflow}
            {retval, state}
        end
      {:no, state} ->
        retval = {:error, :value_not_an_integer_or_out_of_range}
        {retval, state}
    end
  end

  ## ------------------------------------------------------------------
  ## INCRBYFLOAT Command
  ## ------------------------------------------------------------------

  def increment_by_float(key, increment, reply_cb) do
    Exdis.Database.KeyOwner.upsert(key,
      &handle_increment_by_float(&1, increment),
      fn ->
        retval = {:string, float_to_output_string(increment)}
        handle_init(retval, :float, increment)
      end,
      reply_cb)
  end

  defp handle_increment_by_float(string() = state, increment) do
    case maybe_coerce_into_float(state) do
      string(value: value) = state ->
        try do
          value = value + increment
          retval = {:string, float_to_output_string(value)}
          state = string(state, value: value)
          {retval, state}
        rescue
          _ in ArithmeticError ->
            retval = {:error, :increment_would_produce_NaN_or_infinity}
            {retval, state}
        end
      {:no, state} ->
        retval = {:error, :value_not_a_valid_float}
        {retval, state}
    end
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set(key, value, reply_cb) do
    Exdis.Database.KeyOwner.upsert(key,
      &handle_set(&1, value),
      fn -> handle_init(:ok, :binary, value) end,
      reply_cb)
  end

  defp handle_set(_state, new_value) do
    state = string(value: new_value)
    {:ok, state}
  end

  ## ------------------------------------------------------------------
  ## Initialization
  ## ------------------------------------------------------------------

  defp handle_init(retval, repr, value) do
    {retval, string(repr: repr, value: value)}
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Integer
  ## ------------------------------------------------------------------

  defp maybe_coerce_into_integer(string(repr: :integer) = state) do
    state
  end

  defp maybe_coerce_into_integer(string(repr: :binary, value: value) = state) do
    case byte_size(value) < max_integer_value_str_length() and Integer.parse(value) do
      {integer, ""} when integer >= @min_integer_value and integer <= @max_integer_value ->
        string(state, repr: :integer, value: integer)
      _ ->
        {:no, state}
    end
  end

  defp maybe_coerce_into_integer(string(repr: {:iodata,_}, value: value) = state) do
    binary = :erlang.iolist_to_binary(value)
    state = string(state, repr: :binary, value: binary)
    maybe_coerce_into_integer(state)
  end

  defp maybe_coerce_into_integer(string(repr: :float, value: value) = state) do
    case round(value) do
      integer when integer == value ->
        string(state, repr: :integer, value: integer)
      _ ->
        {:no, state}
    end
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Float
  ## ------------------------------------------------------------------

  defp maybe_coerce_into_float(string(repr: :float) = state) do
    state
  end

  defp maybe_coerce_into_float(string(repr: :integer, value: value) = state) do
    case 0.0 + value do
      float when float == value ->
        string(state, repr: :float, value: value)
      _ ->
        # loss of precision
        {:no, state}
    end
  end

  defp maybe_coerce_into_float(string(repr: :binary, value: value) = state) do
    case Float.parse(value) do
      {float, ""} ->
        string(state, repr: :float, value: float)
      _ ->
        {:no, state}
    end
  end

  defp maybe_coerce_into_float(string(repr: {:iodata,_}, value: value) = state) do
    binary = :erlang.iolist_to_binary(value)
    state = string(state, repr: :binary, value: binary)
    maybe_coerce_into_float(state)
  end


  defp float_to_output_string(float) do
    case round(float) do
      integer when integer == float ->
        Integer.to_string(integer)
      _ ->
        inspect float
    end
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - I/O data flattening
  ## ------------------------------------------------------------------

  defp flatten_iodata(string(repr: :binary) = state) do
    state
  end

  defp flatten_iodata(string(repr: {:iodata,_}, value: value) = state) do
    binary = :erlang.iolist_to_binary(value)
    string(state, repr: :binary, value: binary)
  end

  defp flatten_iodata(state) do
    state
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Binary
  ## ------------------------------------------------------------------

  defp coerce_into_binary(string(repr: :binary) = state) do
    state
  end

  defp coerce_into_binary(string(repr: {:iodata,_}, value: value) = state) do
    binary = :erlang.iolist_to_binary(value)
    string(state, repr: :binary, value: binary)
  end

  defp coerce_into_binary(string(repr: :integer, value: value) = state) do
    binary = Integer.to_string(value)
    string(state, repr: :binary, value: binary)
  end

  defp coerce_into_binary(string(repr: :float, value: value) = state) do
    binary = float_to_output_string(value)
    string(state, repr: :binary, value: binary)
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Binary Or I/O data
  ## ------------------------------------------------------------------

  defp coerce_into_binary_or_iodata(string(repr: {:iodata,_}) = state) do
    state
  end

  defp coerce_into_binary_or_iodata(state) do
    coerce_into_binary(state)
  end
end
