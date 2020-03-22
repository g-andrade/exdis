defmodule Exdis.Database.String do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Constants
  ## ------------------------------------------------------------------

  @min_integer_value (-(1 <<< 63))
  @max_integer_value ((1 <<< 63) - 1)

  @max_iodata_fragments_upon_read 100

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:string,
    repr: nil,
    value: nil
  )

  @opaque state :: iodata_state | integer_state | float_state
  @typep iodata_state :: state(:iodata, Exdis.IoData.t)
  @typep integer_state :: state(:integer, integer)
  @typep float_state :: state(:float, float)

  @typep state(repr, value) :: record(:string, repr: repr, value: value)

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

  def max_float_value_str_length() do
    # the DBL_MAX_10_EXP constant in `float.h`
    308
  end

  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append(key, tail) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_append(&1, tail))
  end

  defp handle_append(string() = state, tail) do
    state = coerce_into_iodata(state)
    string(repr: :iodata, value: value) = state
    value = Exdis.IoData.append(value, tail)
    size_after_append = Exdis.IoData.size(value)
    reply = {:integer, size_after_append}
    state = string(state, value: value)
    {:ok_and_update, reply, state}
  end

  defp handle_append(nil, tail) do
    value = Exdis.IoData.new(tail)
    size_after_append = Exdis.IoData.size(value)
    reply = {:integer, size_after_append}
    state = string(repr: :iodata, value: value)
    {:ok_and_update, reply, state}
  end

  defp handle_append(_state, _tail) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GET Command
  ## ------------------------------------------------------------------

  def get(key) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_get/1)
  end

  defp handle_get(string() = state) do
    case maybe_flatten_iodata(state, @max_iodata_fragments_upon_read) do
      string(repr: :iodata, value: value) = state ->
        reply_string = Exdis.IoData.bytes(value)
        reply = {:string, reply_string}
        {:ok_and_update, reply, state}

      string(repr: :integer, value: value) ->
        reply_string = Integer.to_string(value)
        reply = {:string, reply_string}
        {:ok, reply}

      string(repr: :float, value: value) ->
        reply_string = float_to_output_string(value)
        reply = {:string, reply_string}
        {:ok, reply}
    end
  end

  defp handle_get(nil) do
    {:ok, nil}
  end

  defp handle_get(_state) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GETBIT Command
  ## ------------------------------------------------------------------

  def get_bit(key, offset) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_get_bit(&1, offset))
  end

  defp handle_get_bit(string() = state, offset) do
    state = coerce_into_iodata(state)
    string(value: value) = state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    bit_value = Exdis.IoData.get_bit(value, offset)
    reply = {:integer, bit_value}
    {:ok_and_update, reply, state}
  end

  defp handle_get_bit(nil, _offset) do
    reply = {:integer, 0}
    {:ok, reply}
  end

  defp handle_get_bit(_state, _offset) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GETRANGE Command
  ## ------------------------------------------------------------------

  def get_range(key, start, finish) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_get_range(&1, start, finish))
  end

  defp handle_get_range(string() = state, start, finish) do
    state = coerce_into_iodata(state)
    string(value: value) = state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    reply_string = Exdis.IoData.get_range(value, start, finish)
    reply = {:string, reply_string}
    {:ok_and_update, reply, state}
  end

  defp handle_get_range(nil, _start, _finish) do
    reply = {:string, ""}
    {:ok, reply}
  end

  defp handle_get_range(_state, _start, _finish) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by(key, increment) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_increment_by(&1, increment))
  end

  defp handle_increment_by(string() = state, increment) do
    case maybe_coerce_into_integer(state) do
      string(repr: :integer, value: value) = state ->
        case value + increment do
          value when value >= @min_integer_value and value <= @max_integer_value ->
            reply = {:integer, value}
            state = string(state, value: value)
            {:ok_and_update, reply, state}
          _underflow_or_overflow ->
            {:error_and_update, :increment_or_decrement_would_overflow, state}
        end
      state ->
        {:error_and_update, :value_not_an_integer_or_out_of_range, state}
    end
  end

  defp handle_increment_by(nil, increment) do
    reply = {:integer, increment}
    state = string(repr: :integer, value: increment)
    {:ok_and_update, reply, state}
  end

  defp handle_increment_by(_state, _increment) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## INCRBYFLOAT Command
  ## ------------------------------------------------------------------

  def increment_by_float(key, increment) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_increment_by_float(&1, increment))
  end

  defp handle_increment_by_float(string() = state, increment) do
    case maybe_coerce_into_float(state) do
      string(repr: :float, value: value) = state ->
        try do
          value = value + increment
          reply = {:string, float_to_output_string(value)}
          state = string(state, value: value)
          {:ok_and_update, reply, state}
        rescue
          _ in ArithmeticError ->
            {:error_and_update, :increment_would_produce_NaN_or_infinity, state}
        end
      state ->
        {:error_and_update, :value_not_a_valid_float, state}
    end
  end

  defp handle_increment_by_float(nil, increment) do
    reply = {:string, float_to_output_string(increment)}
    state = string(repr: :float, value: increment)
    {:ok_and_update, reply, state}
  end

  defp handle_increment_by_float(_state, _increment) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set(key, value) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_set(&1, value))
  end

  defp handle_set(_state, bytes) do
    value = Exdis.IoData.new(bytes)
    state = string(repr: :iodata, value: value)
    {:ok_and_update, state}
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Integer
  ## ------------------------------------------------------------------

  defp maybe_coerce_into_integer(string(repr: :integer) = state) do
    state
  end

  defp maybe_coerce_into_integer(string(repr: :iodata, value: value) = state) do
    case Exdis.IoData.size(value) <= max_integer_value_str_length() do
      true ->
        value = Exdis.IoData.flatten(value)
        bytes = Exdis.IoData.bytes(value)
        case Integer.parse(bytes) do
          {integer, ""} when integer >= @min_integer_value and integer <= @max_integer_value ->
            string(state, repr: :integer, value: integer)
          _ ->
            string(value: value)
        end
      false ->
        state
    end
  end

  defp maybe_coerce_into_integer(string(repr: :float, value: value) = state) do
    case round(value) do
      integer when integer == value ->
        string(state, repr: :integer, value: integer)
      _ ->
        state
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
        state
    end
  end

  defp maybe_coerce_into_float(string(repr: :iodata, value: value) = state) do
    case Exdis.IoData.size(value) <= max_float_value_str_length() do
      true ->
        value = Exdis.IoData.flatten(value)
        bytes = Exdis.IoData.bytes(value)
        case Float.parse(bytes) do
          {float, ""} ->
            string(state, repr: :float, value: float)
          _ ->
            string(value: value)
        end
      false ->
        state
    end
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

  defp maybe_flatten_iodata(string(repr: :iodata, value: value) = state, max_fragments) do
    case Exdis.IoData.fragments(value) > max_fragments do
      true ->
        value = Exdis.IoData.flatten(value)
        string(state, value: value)
      false ->
        state
    end
  end

  defp maybe_flatten_iodata(state, _max_fragments) do
    state
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To I/O Data
  ## ------------------------------------------------------------------

  defp coerce_into_iodata(string(repr: :iodata) = state) do
    state
  end

  defp coerce_into_iodata(string(repr: :integer, value: value) = state) do
    binary = Integer.to_string(value)
    string(state, repr: :binary, value: binary)
  end

  defp coerce_into_iodata(string(repr: :float, value: value) = state) do
    binary = float_to_output_string(value)
    string(state, repr: :binary, value: binary)
  end
end
