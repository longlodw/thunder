package thunder

const (
	OpEq = OpType(0b0010)
	OpNe = OpType(0b0101)
	OpGt = OpType(0b0100)
	OpLt = OpType(0b0001)
	OpGe = OpType(0b0110)
	OpLe = OpType(0b0011)
)

type OpType uint8

type Op struct {
	Field string
	Value any
	Type  OpType
}

func Eq(field string, value any) Op {
	return Op{
		Field: field,
		Value: value,
		Type:  OpEq,
	}
}

func Ne(field string, value any) Op {
	return Op{
		Field: field,
		Value: value,
		Type:  OpNe,
	}
}

func Gt(field string, value any) Op {
	return Op{
		Field: field,
		Value: value,
		Type:  OpGt,
	}
}

func Lt(field string, value any) Op {
	return Op{
		Field: field,
		Value: value,
		Type:  OpLt,
	}
}

func Ge(field string, value any) Op {
	return Op{
		Field: field,
		Value: value,
		Type:  OpGe,
	}
}

func Le(field string, value any) Op {
	return Op{
		Field: field,
		Value: value,
		Type:  OpLe,
	}
}
