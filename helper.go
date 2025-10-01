package sequel

func (b *builder) addErr(err error) {
	if err == nil {
		return
	}

	if b.err == nil {
		b.err = err
	}
}
