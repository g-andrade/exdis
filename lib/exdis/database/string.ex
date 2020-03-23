defmodule Exdis.Database.String do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Constants
  ## ------------------------------------------------------------------

  @max_iodata_fragments_upon_read 100

  ## ------------------------------------------------------------------
  ## Type and Record Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:string,
    repr: nil,
    value: nil
  )

  @opaque state :: iodata_state | int64_state | float_state
  @typep iodata_state :: state(:iodata, Exdis.IoData.t)
  @typep int64_state :: state(:int64, Exdis.Int64.t)
  @typep float_state :: state(:float, Exdis.Float.t)

  @typep state(repr, value) :: record(:string, repr: repr, value: value)

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
        value_string = Exdis.IoData.bytes(value)
        reply = {:string, value_string}
        {:ok_and_update, reply, state}

      string(repr: :int64, value: value) ->
        value_string = Exdis.Int64.to_decimal_string(value)
        reply = {:string, value_string}
        {:ok, reply}

      string(repr: :float, value: value) ->
        value_string = Exdis.Float.to_decimal_string(value)
        reply = {:string, value_string}
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
    value_string = Exdis.IoData.get_range(value, start, finish)
    reply = {:string, value_string}
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
  ## GETSET Command
  ## ------------------------------------------------------------------

  def get_set(key, value) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_get_set(&1, value))
  end

  defp handle_get_set(string() = state, new_bytes) do
    state = coerce_into_iodata(state)
    string(repr: :iodata, value: old_value) = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    old_bytes = Exdis.IoData.bytes(old_value)
    reply = {:string, old_bytes}

    new_value = Exdis.IoData.new(new_bytes)
    new_state = string(repr: :iodata, value: new_value)
    {:ok_and_update, reply, new_state}
  end

  defp handle_get_set(nil, bytes) do
    value = Exdis.IoData.new(bytes)
    state = string(repr: :iodata, value: value)
    {:ok_and_update, nil, state}
  end

  defp handle_get_set(_state, _new_bytes) do
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
      string(repr: :int64, value: value) = state ->
        case Exdis.Int64.add(value, increment) do
          {:ok, value} ->
            reply = {:integer, value}
            state = string(state, value: value)
            {:ok_and_update, reply, state}
          {:error, :overflow_or_underflow} ->
            {:error_and_update, :increment_or_decrement_would_overflow, state}
        end
      state ->
        {:error_and_update, {:not_an_integer_or_out_of_range, "value"}, state}
    end
  end

  defp handle_increment_by(nil, increment) do
    reply = {:integer, increment}
    state = string(repr: :int64, value: increment)
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
        case Exdis.Float.add(value, increment) do
          {:ok, value} ->
            reply = {:string, Exdis.Float.to_decimal_string(value)}
            state = string(state, value: value)
            {:ok_and_update, reply, state}
          {:error, :NaN_or_infinity} ->
            {:error_and_update, :increment_would_produce_NaN_or_infinity, state}
        end
      state ->
        {:error_and_update, {:not_a_valid_float, "value"}, state}
    end
  end

  defp handle_increment_by_float(nil, increment) do
    value = Exdis.Float.new(increment)
    reply = {:string, Exdis.Float.to_decimal_string(value)}
    state = string(repr: :float, value: value)
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
  ## STRLEN Command
  ## ------------------------------------------------------------------

  def str_length(key) do
    Exdis.Database.KeyOwner.manipulate(key, &handle_str_length/1)
  end

  defp handle_str_length(string(repr: repr, value: value)) do
    case repr do
      :iodata ->
        value_string_length = Exdis.IoData.size(value)
        reply = {:integer, value_string_length}
        {:ok, reply}
      :int64 ->
        value_string_length = Exdis.Int64.decimal_string_length(value)
        reply = {:integer, value_string_length}
        {:ok, reply}
      :float ->
        value_string_length = Exdis.Float.decimal_string_length(value)
        reply = {:integer, value_string_length}
        {:ok, reply}
    end
  end

  defp handle_str_length(nil) do
    reply = {:integer, 0}
    {:ok, reply}
  end

  defp handle_str_length(_state) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Integer
  ## ------------------------------------------------------------------

  defp maybe_coerce_into_integer(string(repr: :int64) = state) do
    state
  end

  defp maybe_coerce_into_integer(string(repr: :iodata, value: value) = state) do
    case Exdis.IoData.size(value) <= Exdis.Int64.max_decimal_string_length() do
      true ->
        value = Exdis.IoData.flatten(value)
        bytes = Exdis.IoData.bytes(value)
        case Exdis.Int64.from_decimal_string(bytes) do
          {:ok, integer} ->
            string(state, repr: :int64, value: integer)
          {:error, _} ->
            string(state, value: value)
        end
      false ->
        state
    end
  end

  defp maybe_coerce_into_integer(string(repr: :float, value: value) = state) do
    case Exdis.Int64.from_float(value) do
      {:ok, integer} ->
        string(state, repr: :int64, value: integer)
      {:error, _} ->
        state
    end
  end

  ## ------------------------------------------------------------------
  ## Type Coercion - To Float
  ## ------------------------------------------------------------------

  defp maybe_coerce_into_float(string(repr: :float) = state) do
    state
  end

  defp maybe_coerce_into_float(string(repr: :int64, value: value) = state) do
    case Exdis.Float.from_integer(value) do
      {:ok, float} ->
        string(state, repr: :float, value: float)
      {:error, _} ->
        state
    end
  end

  defp maybe_coerce_into_float(string(repr: :iodata, value: value) = state) do
    case Exdis.IoData.size(value) <= Exdis.Float.max_decimal_string_length() do
      true ->
        value = Exdis.IoData.flatten(value)
        bytes = Exdis.IoData.bytes(value)
        case Exdis.Float.from_decimal_string(bytes) do
          {:ok, float} ->
            string(state, repr: :float, value: float)
          {:error, _} ->
            string(state, value: value)
        end
      false ->
        state
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

  defp coerce_into_iodata(string(repr: :int64, value: value) = state) do
    bytes = Exdis.Int64.to_decimal_string(value)
    new_value = Exdis.IoData.new(bytes)
    string(state, repr: :iodata, value: new_value)
  end

  defp coerce_into_iodata(string(repr: :float, value: value) = state) do
    bytes = Exdis.Float.to_decimal_string(value)
    new_value = Exdis.IoData.new(bytes)
    string(state, repr: :iodata, value: new_value)
  end
end
