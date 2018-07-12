package index

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("InMemStore", func() {
	var subject *InMemStore

	BeforeEach(func() {
		subject = NewInMemStore()
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("should write/read", func() {
		value := []byte("x")
		Expect(subject.Put([]byte("k1"), value)).To(Succeed())
		Expect(subject.Put([]byte("k2"), value)).To(Succeed())
		Expect(subject.Put([]byte("k1"), []byte("y"))).To(Succeed())
		value[0] = 'z'

		Expect(subject.Get([]byte("k1"))).To(Equal([]byte("y")))
		Expect(subject.Get([]byte("k2"))).To(Equal([]byte("x")))
	})

})
