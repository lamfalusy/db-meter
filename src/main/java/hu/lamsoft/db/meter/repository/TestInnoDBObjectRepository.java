package hu.lamsoft.db.meter.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import hu.lamsoft.db.meter.entity.TestInnoDBObject;

@Repository
public interface TestInnoDBObjectRepository extends JpaRepository<TestInnoDBObject, Long>{

}
