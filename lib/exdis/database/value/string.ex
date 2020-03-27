defmodule Exdis.Database.Value.String do
  require Record

  ## ------------------------------------------------------------------
  ## Constants
  ## ------------------------------------------------------------------

  @max_iodata_fragments_upon_read 100
  @streamable_value_threshold (64 * 1024) # FIXME

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

  def append(key_owner, tail) do
    Exdis.Database.KeyOwner.write(key_owner, &handle_append(&1, tail))
  end

  defp handle_append(string() = state, tail) do
    state = coerce_into_iodata(state)
    string(repr: :iodata, value: value) = state
    value = Exdis.IoData.append(value, tail)
    size_after_append = Exdis.IoData.size(value)
    reply_value = {:integer, size_after_append}
    state = string(state, value: value)
    {:ok, reply_value, state}
  end

  defp handle_append(nil, tail) do
    value = Exdis.IoData.new(tail)
    size = Exdis.IoData.size(value)
    reply_value = {:integer, size}
    state = string(repr: :iodata, value: value)
    {:ok, reply_value, state}
  end

  defp handle_append(_state, _tail) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## BITCOUNT Command
  ## ------------------------------------------------------------------

  def bit_count(key_owner, start, finish) do
    Exdis.Database.KeyOwner.read(key_owner, &handle_bit_count(&1, start, finish))
  end

  defp handle_bit_count(string() = state, start, finish) do
    state = coerce_into_iodata(state)
    string(value: value) = state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    bit_count = Exdis.IoData.bit_count(value, start, finish)
    reply_value = {:integer, bit_count}
    {:ok, reply_value, state}
  end

  defp handle_bit_count(nil, _start, _finish) do
    reply_value = {:integer, 0}
    {:ok, reply_value}
  end

  defp handle_bit_count(_state, _start, _finish) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## BITPOS Command
  ## ------------------------------------------------------------------

  def bit_position(key_owner, bit, start \\ 0, finish \\ nil) do
    Exdis.Database.KeyOwner.read(key_owner, &handle_bit_position(&1, bit, start, finish))
  end

  defp handle_bit_position(string() = state, bit, start, finish) do
    state = coerce_into_iodata(state)
    string(value: value) = state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    _value_size = Exdis.IoData.size(value)

    case Exdis.IoData.bit_position(value, bit, start, finish) do
      {:found, offset} ->
        reply_value = {:integer, offset}
        {:ok, reply_value, state}
      {:skipped, _} when bit === 1 or finish !== nil ->
        reply_value = {:integer, -1}
        {:ok, reply_value, state}
      {:skipped, offset} ->
        reply_value = {:integer, offset}
        {:ok, reply_value, state}
    end
  end

  defp handle_bit_position(nil, bit, _start, _finish) do
    case bit do
      0 ->
        {:ok, {:integer, 0}}
      1 ->
        {:ok, {:integer, -1}}
    end
  end

  defp handle_bit_position(_state, _bit, _start, _finish) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GET Command
  ## ------------------------------------------------------------------

  def get(key_owner) do
    Exdis.Database.KeyOwner.read(key_owner, &handle_get/1)
  end

  defp handle_get(string() = state) do
    state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    reply_value = maybe_string_stream(state)
    {:ok, reply_value, state}
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

  def get_bit(key_owner, offset) do
    Exdis.Database.KeyOwner.read(key_owner, &handle_get_bit(&1, offset))
  end

  defp handle_get_bit(string() = state, offset) do
    state = coerce_into_iodata(state)
    string(value: value) = state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    bit_value = Exdis.IoData.get_bit(value, offset)
    reply_value = {:integer, bit_value}
    {:ok, reply_value, state}
  end

  defp handle_get_bit(nil, _offset) do
    reply_value = {:integer, 0}
    {:ok, reply_value}
  end

  defp handle_get_bit(_state, _offset) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GETRANGE Command
  ## ------------------------------------------------------------------

  def get_range(key_owner, start, finish) do
    Exdis.Database.KeyOwner.read(key_owner, &handle_get_range(&1, start, finish))
  end

  defp handle_get_range(string() = state, start, finish) do
    state = coerce_into_iodata(state)
    string(value: value) = state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    value_string = Exdis.IoData.get_range(value, start, finish)
    reply_value = {:string, value_string}
    {:ok, reply_value, state}
  end

  defp handle_get_range(nil, _start, _finish) do
    reply_value = {:string, ""}
    {:ok, reply_value}
  end

  defp handle_get_range(_state, _start, _finish) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## GETSET Command
  ## ------------------------------------------------------------------

  def get_set(key_owner, new_bytes) do
    Exdis.Database.KeyOwner.write(key_owner, &handle_get_set(&1, new_bytes))
  end

  defp handle_get_set(string() = state, new_bytes) do
    state = maybe_flatten_iodata(state, @max_iodata_fragments_upon_read)
    reply_value = maybe_string_stream(state)

    new_value = Exdis.IoData.new(new_bytes)
    new_state = string(repr: :iodata, value: new_value)
    {:ok, reply_value, new_state}
  end

  defp handle_get_set(nil, bytes) do
    value = Exdis.IoData.new(bytes)
    state = string(repr: :iodata, value: value)
    {:ok, nil, state}
  end

  defp handle_get_set(_state, _new_bytes) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by(key_owner, increment) do
    Exdis.Database.KeyOwner.write(key_owner, &handle_increment_by(&1, increment))
  end

  defp handle_increment_by(string() = state, increment) do
    case maybe_coerce_into_integer(state) do
      string(repr: :int64, value: value) = state ->
        case Exdis.Int64.add(value, increment) do
          {:ok, value} ->
            reply_value = {:integer, value}
            state = string(state, value: value)
            {:ok, reply_value, state}
          {:error, :overflow_or_underflow} ->
            {:error, :increment_or_decrement_would_overflow, state}
        end
      state ->
        {:error, {:not_an_integer_or_out_of_range, "value"}, state}
    end
  end

  defp handle_increment_by(nil, increment) do
    reply_value = {:integer, increment}
    state = string(repr: :int64, value: increment)
    {:ok, reply_value, state}
  end

  defp handle_increment_by(_state, _increment) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## INCRBYFLOAT Command
  ## ------------------------------------------------------------------

  def increment_by_float(key_owner, increment) do
    Exdis.Database.KeyOwner.write(key_owner, &handle_increment_by_float(&1, increment))
  end

  defp handle_increment_by_float(string() = state, increment) do
    case maybe_coerce_into_float(state) do
      string(repr: :float, value: value) = state ->
        case Exdis.Float.add(value, increment) do
          {:ok, value} ->
            reply_value = {:string, Exdis.Float.to_decimal_string(value)}
            state = string(state, value: value)
            {:ok, reply_value, state}
          {:error, :NaN_or_infinity} ->
            {:error, :increment_would_produce_NaN_or_infinity, state}
        end
      state ->
        {:error, {:not_a_valid_float, "value"}, state}
    end
  end

  defp handle_increment_by_float(nil, increment) do
    value = Exdis.Float.new(increment)
    reply_value = {:string, Exdis.Float.to_decimal_string(value)}
    state = string(repr: :float, value: value)
    {:ok, reply_value, state}
  end

  defp handle_increment_by_float(_state, _increment) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## MGET Command
  ## ------------------------------------------------------------------

  def mget(key_owners) do
    reply_array_members =
      Enum.map(key_owners,
        fn key_owner ->
          case Exdis.Database.KeyOwner.read(key_owner, &handle_get(&1)) do
            {:ok, reply} ->
              reply
            {:error, :key_of_wrong_type} ->
              nil
          end
        end)

    {:success_array, reply_array_members}
  end

  ## ------------------------------------------------------------------
  ## MSET Command
  ## ------------------------------------------------------------------

  def mset(key_owners, values) do
    mset_recur(key_owners, values)
  end

  defp mset_recur([key_owner|next_key_owners], [value|next_values]) do
    :ok = Exdis.Database.KeyOwner.write(key_owner, &handle_set(&1, value))
    mset_recur(next_key_owners, next_values)
  end

  defp mset_recur([], []) do
    :ok
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set(key_owner, bytes) do
    Exdis.Database.KeyOwner.write(key_owner, &handle_set(&1, bytes))
  end

  defp handle_set(_state, bytes) do
    value = Exdis.IoData.new(bytes)
    reply_value = :ok
    state = string(repr: :iodata, value: value)
    {:ok, reply_value, state}
  end

  ## ------------------------------------------------------------------
  ## STRLEN Command
  ## ------------------------------------------------------------------

  def str_length(key_owner) do
    Exdis.Database.KeyOwner.read(key_owner, &handle_str_length/1)
  end

  defp handle_str_length(string(repr: repr, value: value)) do
    case repr do
      :iodata ->
        value_string_length = Exdis.IoData.size(value)
        reply_value = {:integer, value_string_length}
        {:ok, reply_value}
      :int64 ->
        value_string_length = Exdis.Int64.decimal_string_length(value)
        reply_value = {:integer, value_string_length}
        {:ok, reply_value}
      :float ->
        value_string_length = Exdis.Float.decimal_string_length(value)
        reply_value = {:integer, value_string_length}
        {:ok, reply_value}
    end
  end

  defp handle_str_length(nil) do
    reply_value = {:integer, 0}
    {:ok, reply_value}
  end

  defp handle_str_length(_state) do
    {:error, :key_of_wrong_type}
  end

  ## ------------------------------------------------------------------
  ## Value Streams
  ## ------------------------------------------------------------------

  def maybe_string_stream(string(repr: :iodata, value: value)) do
    size = Exdis.IoData.size(value)
    fragments = Exdis.IoData.fragments(value)

    case size <= @streamable_value_threshold or fragments < 2 do
      true ->
        bytes = Exdis.IoData.bytes(value)
        {:string, bytes}
      false ->
        bytes = Exdis.IoData.bytes(value)
        {:stream, Exdis.Database.Value.Stream.new(&start_consuming_string_stream(&1, size, bytes))}
    end
  end

  def maybe_string_stream(string(repr: :int64, value: value)) do
    value_string = Exdis.Int64.to_decimal_string(value)
    {:string, value_string}
  end

  def maybe_string_stream(string(repr: :float, value: value)) do
    value_string = Exdis.Float.to_decimal_string(value)
    {:string, value_string}
  end

  defp start_consuming_string_stream(nil, size, bytes) do
    case take_first_chunk_from_bytes(bytes) do
      {chunk, [_|_] = remaining} ->
        part = {:partial, {:string_start, size, chunk}}
        continuation_callback = &continue_consuming_string_stream(&1, remaining)
        {:more, part, continuation_callback}
      {chunk, [] = _remaining} ->
        part = {:string, chunk}
        {:finished, part}
    end
  end

  defp continue_consuming_string_stream(nil, bytes) do
    case take_first_chunk_from_bytes(bytes) do
      {chunk, [_|_] = remaining} ->
        part = {:partial, {:string_continue, chunk}}
        continuation_callback = &continue_consuming_string_stream(&1, remaining)
        {:more, part, continuation_callback}
      {chunk, [] = _remaining} ->
        part = {:partial, {:string_finish, chunk}}
        {:finished, part}
    end
  end

  defp take_first_chunk_from_bytes([head|next]) do
    case take_first_chunk_from_bytes(head) do
      {chunk, [_,_|_] = head_remaining} ->
        {chunk, [head_remaining | next]}
      {chunk, [head_remaining]} ->
        {chunk, [head_remaining | next]}
      {chunk, []} ->
        {chunk, next}
    end
  end

  defp take_first_chunk_from_bytes([]) do
    {"", []}
  end

  defp take_first_chunk_from_bytes(<<binary :: bytes>>) do
    {binary, []}
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
